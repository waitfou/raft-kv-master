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
import com.wangguo.java.raft.server.rpc.DefaultRpcServiceImpl;
import com.wangguo.java.raft.server.rpc.RpcService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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


    private HeartBeatTask heartBeatTask = new HeartBeatTask();
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
    public RpcService rpcServer;


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
        rpcServer.init();
        rpcClient.init();

        //一致性模块初始化
        consensus = new DefaultConsensus(this); //告诉DefultConsensus类是哪个节点在调用它
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

    //每当创建一个节点都要对该节点进行配置，对他的PeerSet，logModule属性等赋予初始值
    @Override
    public void setConfig(NodeConfig config) {
        this.config = config;
        //根据节点的配置信息获取相应的状态机
        stateMachine = StateMachineSaveType.getForType(config.getStateMachineSaveType()).getStateMachine();
        logModule = DefaultLogModule.getInstance();

        peerSet = PeerSet.getInstance();
        for (String s : config.getPeerAddrs()){
            Peer peer = new Peer(s);
            peerSet.addPeer(peer);
            if (s.equals("localhost:" + config.getSelfPort())){
                peerSet.setSelf(peer); // 在peerSet中标识一下自己
            }
        }
        // 给该节点配置rpc服务
        rpcServer = new DefaultRpcServiceImpl(config.selfPort, this);
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

    /**
     * 客户端的请求发送到服务器节点之后，服务器节点对请求进行处理
     * @param request
     * @return
     */
    @Override
    public ClientKVAck handlerClientRequest(ClientKVReq request) {
        // request是请求类型 有PUT和GET两种。getKey是返回键
        log.warn("handlerClientRequest handler {} operation, and key : [{}], value : [{}]",
                ClientKVReq.Type.value(request.getType()), request.getKey(), request.getValue());
        // 如果来自客户端的请求到达非领导节点，那么该节点要把请求转发（重定向）到领导节点
        if (status != NodeStatus.LEADER) {
            log.warn("I not am leader, only invoke redirect method, leader addr : {}, my addr: {}", peerSet.getLeader(), peerSet.getSelf().getAddr());
            return redirect(request); //重定向请求到Leader节点
        }
        // 能运行到下面的代码的一定是Leader节点。
        // 如果客户端的请求是get请求，那么就就查找并返回给客户端他想要获取的信息
        if (request.getType() == ClientKVReq.GET){
            LogEntry logEntry = stateMachine.get(request.getKey());
            if (logEntry != null){
                return new ClientKVAck(logEntry);
            }
            return new ClientKVAck(null);//如果没找到，返回null
        }
        // 如果是PUT请求，那么久生成日志实体
        LogEntry logEntry = LogEntry.builder()
                .command(Command.builder(). //客户端发送的键值对
                        key(request.getKey()).
                        value(request.getValue()).
                        build())
                .term(currentTerm) // 当前任期号
                .build();

        // 预提交到本地日志， TODO 预提交
        logModule.write(logEntry);
        log.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());
        // 原子操作类型数据，为了保证多线程操作该数据的时候，数据得到正确的结果，因为后面会同时创建多个发起RPC复制的线程，都会对该变量进行操作
        // 因此要用原子类
        final AtomicInteger success = new AtomicInteger(0);
        // 我有一个任务，提交给了Future，Future替我完成这个任务。期间我自己可以去做
        // 任何想做的事情。一段时间之后，我就便可以从Future那儿取出结果。
        // 就相当于下了一张订货单，一段时间后可以拿着提订单来提货，这期间可以干别的任何事情。
        // 其中Future 接口就是订货单，真正处理订单的是Executor类，它根据Future接口的要求来生产产品。
        List<Future<Boolean>> futureList = new ArrayList<>();

        int count = 0;
        // 把日志复制到别的机器
        for(Peer peer : peerSet.getPeersWithOutSelf()) {
            count ++; //统计服务器集群中有多少个节点
            // 并行发起RPC复制，也就是把给每个节点复制日志的任务存放到futurelist中，主线程就可以执行别的任务
            // 过段时间来看看任务有没有执行完毕就行。
            futureList.add(replication(peer, logEntry));
        }
        // 在构造CountDownLatch的时候需要传入一个整数n，在这个整数倒数到0之前，主线程需要等待在门口
        // 这个倒数过程是由各个执行线程驱动的，每个线程执行完任务“倒数”一次。
        // CountDownLatch()的括号里是几，表示latch.countDown()要执行几次才能唤醒主线程
        CountDownLatch latch = new CountDownLatch(futureList.size());
        // 写时复制，ArrayList的线程安全版本
        List<Boolean> resultList = new CopyOnWriteArrayList<>();
        getRPCAppendResult(futureList, latch, resultList);

        try {
            //使当前线程等待，直到锁存器倒计时为零，除非线程被中断或指定的等待时间已过。
            latch.await(4000, MILLISECONDS);
        } catch (InterruptedException e){
            log.error(e.getMessage(), e);
        }

        // resultList中存放的是给每个节点复制日志是否成功了的信息
        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                // incrementAndGet方法在一个无限循环体内，不断重试将一个比当前值大一的新值赋给自己，如果失败
                // ，说明已经被其他线程修改过，于是再次进入循环下一次操作
                success.incrementAndGet();
            }
        }

        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values()); //matchIndexs.values是获得每个服务器的最高索引值的一个集合
        int median = 0;
        if (matchIndexList.size() >= 2) {
            Collections.sort(matchIndexList);
            median = matchIndexList.size() / 2;
        }
        // 获取matchIndexList列表排序之后中间位置的元素
        Long N = matchIndexList.get(median);
        if(N > commitIndex) {
            LogEntry entry = logModule.read(N);
            // 这个主要是判断预提交日志是否提交到这个位置（N）了。
            if(entry != null && entry.getTerm() == currentTerm) {
                commitIndex = N;
            }
        }
        // 只要超过一半的节点成功响应，那么就是复制成功了的
        if (success.get() >= (count / 2)) {  //success是AtomicInteger原子类，.get方法是获取原子类的值
            commitIndex = logEntry.getIndex();
            // 将日志提交应用到该状态机
            getStateMachine().apply(logEntry);
            lastApplied = commitIndex;
            log.info("success apply local state machine, logEntry info : {}", logEntry);
            return ClientKVAck.ok();
        }else {
            // 如果没有接收到一半以上的成功信息，那么就进行回滚
            logModule.removeOnStartIndex(logEntry.getIndex());
            log.warn("fail apply local state machine, logEntry info : {}", logEntry);
            // TODO 不应用到状态机，但已经记录到日志中，由定时任务从重试队列取出，然后重复尝试，当达到条件时，应用到状态机。
            return ClientKVAck.fail();
        }
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
            //如果不是领导者就返回，因为只有领导者需要不断发送心跳信息
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
                    try{
                        AentryResult aentryResult = getRpcClient().send(request);
                        long term = aentryResult.getTerm();
                        // 如果Follower节点的term比我这个领导者还大，那么我这个领导者就要变成follower
                        if(term>currentTerm){
                            log.error("self will become follower, he's term : {}, my term : {}", term, currentTerm);
                            currentTerm = term;
                            votedFor = "";
                            status = NodeStatus.FOLLOWER;
                        }
                    } catch (Exception e) {
                        log.error("heartBeatTask RPC Fail, request URL : {}", request.getUrl());
                    }
                },false);
            }
        }
    }

    /**
     * 向peer机器中复制entry日志实体
     */
    public Future<Boolean> replication(Peer peer, LogEntry entry){
        // 向线程池中提交一个需要返回结果的任务
        return RaftThreadPool.submit(()->{
            long start = System.currentTimeMillis(), end = start;
            while(end - start < 20 * 1000L){
                // 设置日志追加RPC的参数
                AentryParam aentryParam = AentryParam.builder().build();
                aentryParam.setTerm(currentTerm);
                aentryParam.setServerId(peer.getAddr());
                aentryParam.setLeaderId(peerSet.getSelf().getAddr());
                aentryParam.setLeaderCommit(commitIndex);

                long nextIndex = nextIndexs.get(peer); //获取该节点的下一个索引
                // logEntries 用于存放要向跟随者节点中追加的日志信息
                LinkedList<LogEntry> logEntries = new LinkedList<>();
                // 如果待复制的日志列表中的日志大于等于该节点需要的下一个索引，那么就要向该节点中复制日志
                if(entry.getIndex() >= nextIndex){
                    for (long i = nextIndex; i <= entry.getIndex(); i++){
                        LogEntry l = logModule.read(i); // 依据索引从日志模块中读取日志
                        if(l != null) {
                            // 将要向该节点复制的日志放入一个链表中
                            logEntries.add(l);
                        }
                    }
                }else{
                    logEntries.add(entry);
                }

                //
                LogEntry preLog = getPreLog(logEntries.getFirst()); //logEntries.getFirst是获取该链表中的第一个节点。
                aentryParam.setPreLogTerm(preLog.getTerm()); //上一个日志实体的任期号
                aentryParam.setPreLogIndex(preLog.getIndex());

                aentryParam.setEntries(logEntries.toArray(new LogEntry[0]));
                // 包装请求
                Request request = Request.builder()
                        .cmd(Request.A_ENTRIES)
                        .obj(aentryParam)
                        .url(peer.getAddr())
                        .build();

                try {
                    AentryResult result = getRpcClient().send(request);
                    if (result == null) {
                        return false;
                    }
                }
            }
        });
    }

    private void getRPCAppendResult(List<Future<Boolean>> futureList, CountDownLatch latch, List<Boolean> resultList) {
        for(Future<Boolean> future : futureList) {
            // 执行线程池中的任务
            RaftThreadPool.execute(() ->{
                try {
                    resultList.add(future.get(3000, MILLISECONDS));
                } catch (Exception e){
                    log.error(e.getMessage(), e);
                    resultList.add(false);
                } finally {
                    // 唤醒阻塞线程的作用，如果当计数器的值减到0，那么主线程就会继续往下执行。
                    latch.countDown();
                }
            });
        }
    }

    /**
     * 获取该日志索引前的一个索引位置的日志
     * @param logEntry
     * @return
     */
    private LogEntry getPreLog(LogEntry logEntry){
        LogEntry entry = logModule.read(logEntry.getIndex() - 1);

        if (entry == null) {
            log.warn("get preLog is null, parameter logEntry : {}", logEntry);
            entry = LogEntry.builder().index(0L).term(0).command(null).build();
        }
        return entry; //返回一个空的实体
    }
}
