package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.entity.*;
import com.wangguo.java.raft.common.rpc.DefaultRpcClient;
import com.wangguo.java.raft.common.rpc.Request;
import com.wangguo.java.raft.common.rpc.RpcClient;
import com.wangguo.java.raft.server.*;
import com.wangguo.java.raft.server.changes.ClusterMembershipChanges;
import com.wangguo.java.raft.server.changes.Result;
import com.wangguo.java.raft.server.constant.StateMachineSaveType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 抽象机器节点，初始化为follower,角色随时变化
 */
@Getter
@Setter
@Slf4j
public class DefaultNode implements Node, ClusterMembershipChanges {
    /**
     * 选举时间
     */
    public volatile long electionTime = 15*1000;
    public PeerSet peerSet;

    volatile boolean running = false;
    public DefaultNode() {

    }
    public static DefaultNode getInstance(){
        return DefaultNodeLazyHolder.INSTANCE;
    }

    private static class DefaultNodeLazyHolder{
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
    /* ========================== 所有服务器上持久存在的 ======================= */
    /**
     * 服务器最后一次知道的任期号（初始化为0，持续递增）
     */
    volatile long currentTerm = 0;
    /**
     * 在当前获得选票的候选人Id
     */
    volatile String votedFor;
    /**
     * 日志条目集：每一个条目包含一个用户状态机执行的指令，和收到的任期号
     */
    LogModule logModule;

    /* ========================== 所有服务器上经常变的 ======================= */







    /* ==================================================================== */

    public RpcClient rpcClient = new DefaultRpcClient();
    public NodeConfig config;
    public StateMachine stateMachine;




    /* ==================================================================== */
    /**
     * 服务端RPC
     */
    public RpcService rpcService;
    ClusterMembershipChanges delegate;

    /**
     * 客户端RPC
     */
    public RpcClient rpcClient = new DefaultRpcClient();
    /**
     * 默认节点初始化时的一些操作
     * @throws Throwable
     */
    @Override
    public void init() throws Throwable {
        //设置节点的运行状态为true
        running = true;
        rpcService.init();
        rpcClient.init();

        consensus = new DefaultConsensus(this);
        /**
         * 集群变动的代理
         */
        delegate = new ClusterMembershipChangesImpl(this);

        RaftThreadPool
        LogEntry logEntry = logModule.getLast();
        if(logEntry!=null){
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
    }

    @Override
    public Result addPeer(Peer newPeer) {
        return null;
    }

    /**
     * 一致性模块实现
     */
    Consensus consensus;
    @Override
    public Result removePeer(Peer oldPeer) {
        return null;
    }

    public RvoteResult handlerRequestVote(RvoteParam param){
        log.warn("handlerRequestVote will be invoke, param info :{}", param);
        /**
         * 根据共识算法进行投票
         */
        return consensus.requestVote(param);
    }
    public AentryResult handlerAppendEntries(AentryParam param){
        /**
         * 如果追加日志的的日志实体不为空
         */
        if(param.getEntries()!=null){
            log.warn("node receive node {} append entry, entry content = {}", param.getLeaderId(), param.getEntries());
        }
        return consensus.appendEntries(param);
    }

    @Override
    public ClientKVAck handlerClientRequest(ClientKVReq request) {

    }

    /**
     * 如果客户端请求发送到了跟随者节点，那么跟随者节点把请求转发给领导者节点
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
}
