package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.entity.*;
import com.wangguo.java.raft.common.rpc.DefaultRpcClient;
import com.wangguo.java.raft.common.rpc.Request;
import com.wangguo.java.raft.common.rpc.RpcClient;
import com.wangguo.java.raft.server.*;
import com.wangguo.java.raft.server.changes.ClusterMembershipChanges;
import com.wangguo.java.raft.server.changes.Result;
import com.wangguo.java.raft.server.constant.StateMachineSaveType;
import com.wangguo.java.raft.server.current.RaftThreadPool;
import com.wangguo.java.raft.server.rpc.RpcService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * 抽象机器节点，初始化为follower,角色随时变化
 */
@Getter
@Setter
@Slf4j
public class DefaultNode implements Node, ClusterMembershipChanges {
    /**
     * 选举时间间隔基数
     */
    public volatile long electionTime = 15 * 1000;
    /**
     * 上一次选举时间
     */
    public volatile long preElectionTime = 0;
    /**
     * 上一次心跳时间戳
     */
    public volatile long preHeartBeatTime = 0;
    /**
     * 心跳间隔基数
     */
    public final long heartBeatTick = 5 * 100;


    private HeartBeatTask heartBeatTask =
    public DefaultNode() {

    }

    public static DefaultNode getInstance() {
        return DefaultNodeLazyHolder.INSTANCE;
    }

    /**
     * 懒汉模式
     */
    private static class DefaultNodeLazyHolder {
        private static final DefaultNode INSTANCE = new DefaultNode();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    /* =============================== 节点当前状态 ======================= */
    public volatile int status = NodeStatus.FOLLOWER; //节点角色
    public PeerSet peerSet; //节点的集群集
    volatile boolean running = false; //节点的运行状态
    /* ========================== 所有服务器上持久存在的 ======================= */
    /**
     * 服务器最后一次知道的任期号（初始化为0，持续递增）
     */
    volatile long currentTerm = 0;
    /**
     * 在当前获得选票的候选人Id，也就是该节点把票投给了谁
     */
    volatile String votedFor;
    /**
     * 日志条目集：每一个条目包含一个用户状态机执行的指令，和收到的任期号
     */
    LogModule logModule;

    /* ========================== 所有服务器上经常变的 ======================= */
    /**
     * 已知的最大的已经提交的日志条目的索引值
     */
    volatile long commitIndex;
    /**
     * 最后被应用到状态机的日志条目索引值（初始化为0，持续递增）
     */
    volatile long lastApplied = 0;
    /* ================ 在领导人里经常改变的（选举后重新初始化） ================= */

    /**
     * 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加1？？？？）
     */
    Map<Peer, Long> nextIndexs;
    /**
     * 对于每一个服务器，已经复制给他的日志的最高索引值
     */
    Map<Peer, Long> matchIndexs;
    /* ==================================================================== */

    /**
     * 客户端RPC
     */
    public RpcClient rpcClient = new DefaultRpcClient();
    public NodeConfig config;
    public StateMachine stateMachine;
    /**
     * 服务端RPC
     */
    public RpcService rpcService;


    /* ==================================================================== */

    ClusterMembershipChanges delegate;
    /**
     * 一致性模块实现
     */
    Consensus consensus;
    /* ==================================================================== */

    /**
     * 默认节点初始化时的一些操作
     *
     * @throws Throwable
     */
    @Override
    public void init() throws Throwable {
        //设置节点的运行状态为true
        running = true;
        //RPC服务初始化
        rpcService.init();
        rpcClient.init();

        //一致性模块初始化
        consensus = new DefaultConsensus(this);
        delegate = new ClusterMembershipChangesImpl(this);
        /**
         * 集群变动的代理
         */
        delegate = new ClusterMembershipChangesImpl(this);

        /**
         * 线程池初始化
         */
        RaftThreadPool.scheduleWithFixedDelay();

        LogEntry logEntry = logModule.getLast();
        if (logEntry != null) {
            //日志实体中获取当前任期号
            currentTerm = logEntry.getTerm();
        }
        log.info("start success, selfId:{}", peerSet.getSelf());
    }

    @Override
    public void destroy() throws Throwable {

    }

    //对默认节点进行配置
    @Override
    public void setConfig(NodeConfig config) {
        this.config = config;
        //根据节点的配置信息获取相应的状态机
        stateMachine = StateMachineSaveType.getForType(config.getStateMachineSaveType()).getStateMachine();
        logModule = DefaultLogModule.
    }

    @Override
    public Result addPeer(Peer newPeer) {
        return null;
    }


    @Override
    public Result removePeer(Peer oldPeer) {
        return null;
    }

    public RvoteResult handlerRequestVote(RvoteParam param) {
        log.warn("handlerRequestVote will be invoke, param info :{}", param);
        /**
         * 根据共识算法进行投票
         */
        return consensus.requestVote(param);
    }

    public AentryResult handlerAppendEntries(AentryParam param) {
        /**
         * 如果追加日志的的日志实体不为空
         */
        if (param.getEntries() != null) {
            log.warn("node receive node {} append entry, entry content = {}", param.getLeaderId(), param.getEntries());
        }
        return consensus.appendEntries(param);
    }

    @Override
    public ClientKVAck handlerClientRequest(ClientKVReq request) {

    }

    /**
     * 如果客户端请求发送到了跟随者节点，那么跟随者节点把请求转发给领导者节点
     *
     * @param request
     * @return
     */
    @Override
    public ClientKVAck redirect(ClientKVReq request) {
        Request r = Request.builder()
                .obj(request)
                .url(peerSet.getLeader().getAddr())
                .cmd(Request.CLIENT_REQ).build();
        /**重定向转发给Leader，还是以客户端的方式封装*/
        return rpcClient.send(r);
    }

    /**
     * 领导者节点持续向FOLLWER节点发送心跳
     */
    class HeartBeatTask implements Runnable{
        @Override
        public void run() {
            //如果不是领导者
            if(status != NodeStatus.LEADER){
                return;
            }
            long current = System.currentTimeMillis();
            //如果距离上次的心跳时间的长度没有超过阈值
            if(current - preHeartBeatTime < heartBeatTick){
                return;
            }
            log.info("====================== NextIndex ===================");
            for(Peer peer : peerSet.getPeersWithOutSelf()){ //集群中其他的服务器节点
                log.info("Peer {} nextIndex={}", peer.getAddr(), nextIndexs.get(peer));
            }
            preHeartBeatTime = System.currentTimeMillis();
            /**
             * 发送日志时对RPC追加日志的参数进行初始化
             */
            for(Peer peer : peerSet.getPeersWithOutSelf()){
                AentryParam param = AentryParam.builder()
                        .entries(null) //心跳，空日志
                        .leaderId(peerSet.getSelf().getAddr())
                        .serverId(peer.getAddr())
                        .term(currentTerm)
                        .leaderCommit(commitIndex)
                        .build();
                //包装一个附加日志的请求
                Request request = new Request(
                        Request.A_ENTRIES,
                        param,
                        peer.getAddr()
                );
                RaftThreadPool.execute(()->{

                });
            }
        }
    }

}
