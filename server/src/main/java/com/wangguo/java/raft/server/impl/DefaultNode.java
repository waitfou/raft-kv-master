package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.RaftRemotingException;
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
import com.wangguo.java.raft.server.util.LongConvert;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
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

    private ElectionTask electionTask = new ElectionTask();

    private ReplicationFailQueueConsumer replicationFailQueueConsumer = new ReplicationFailQueueConsumer();

    private LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue = new LinkedBlockingQueue<>(2048);
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
     * 默认节点初始化时的一些操作，在启动类中调用该节点的init方法对节点进行一些其他初始化
     * @throws Throwable
     */
    @Override
    public void init() throws Throwable {
        //设置节点的运行状态为true
        running = true;
        //RPC服务初始化
        rpcServer.init();
        rpcClient.init();

        //创建专属于自己的一致性模块
        consensus = new DefaultConsensus(this); //告诉DefultConsensus类是哪个节点在调用它
        /**
         * 创建专属于自己的集群变动代理
         */
        delegate = new ClusterMembershipChangesImpl(this);


        /**
         * 创建并执行一个周期性操作，该操作在给定的初始延迟后首先启用，然后在一次执行终止和下一次执行开始之间
         * 的给定延迟内启用。如果任务的任何执行遇到异常，则会抑制后续执行。否则，任务将仅通过取消或终止执行器而终止
         */
        // 在线程中添加一个周期性执行的任务。有4个参数 command, initialDelay, delay, unit 分别是
        // 要执行的任务、第一次延迟延迟的时间、一次执行的结束到下一次执行的开始的时间、时间单位
        // leader节点每过500ms就要向其他节点发送一次心跳。告诉别人leader节点还在。以免follower发起新的选举
        RaftThreadPool.scheduleWithFixedDelay(heartBeatTask, 500);
        // 集群刚刚启动的时候，就是通过electionTask完成第一个leader的选举的
        RaftThreadPool.scheduleAtFixedRate(electionTask, 6000, 500);
        // 这个暂时忽略，是算法的细节的优化
        RaftThreadPool.execute(replicationFailQueueConsumer);

        LogEntry logEntry = logModule.getLast();
        if (logEntry != null) {
            //日志实体中获取当前任期号
            currentTerm = logEntry.getTerm();
        }
        log.info("start success, selfId:{}", peerSet.getSelf());
    }

    @Override
    public void destroy() throws Throwable {
        rpcServer.destroy();
        stateMachine.destroy();
        rpcClient.destroy();
        running = false;
        log.info("destory success");
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
        // 通过一致性模块向follower节点中追加日志
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
            // 将日志提交应用到该状态机（在这里是向leader状态机中提交）
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

                // 获取Leader节点中要复制的节点的前一个日志信息
                LogEntry preLog = getPreLog(logEntries.getFirst()); //logEntries.getFirst是获取该链表中的第一个节点。
                aentryParam.setPreLogTerm(preLog.getTerm()); //Leader待复制的日志实体的上一个的任期号
                aentryParam.setPreLogIndex(preLog.getIndex()); //Leader待复制的日志实体的上一个的索引号

                // 参数是aentryParam就是复制日志的请求
                aentryParam.setEntries(logEntries.toArray(new LogEntry[0]));
                // 包装请求
                Request request = Request.builder()
                        .cmd(Request.A_ENTRIES)
                        .obj(aentryParam)
                        .url(peer.getAddr())
                        .build();

                try {
                    // 返回复制日志的结果
                    AentryResult result = getRpcClient().send(request);
                    if (result == null) {
                        return false;
                    }
                    if (result.isSuccess()) {
                        log.info("append follower entry success, follower=[{}], entry=[{}]", peer, aentryParam.getEntries());
                        // 更新当前的Follwer节点需要的下一个索引的日志的索引号
                        nextIndexs.put(peer, entry.getIndex() + 1);
                        // 更新已经复制给当前Follwer节点的最大的索引号
                        matchIndexs.put(peer, entry.getIndex());
                        return true;
                    } else if (!result.isSuccess()) { // 如果没有复制成功
                        if (result.getTerm() > currentTerm) { // 如果是因为follower的任期号比我这个leader节点的任期号还要大的原因导致的复制失败。
                            // 也就是当领导者发现自己的任期编号比其他节点还要小的时候，则会立即更新自己为跟随者
                            log.warn("follower [{}] term [{}] than more self, and my term = [{}], so, I will become follower", peer, result.getTerm(), currentTerm);
                            currentTerm = result.getTerm();

                            // 认怂， 我这个leader变成跟随者，这是什么情况？？
                            status = NodeStatus.FOLLOWER;
                            return false;
                        }
                        else {
                            if (nextIndex == 0) {
                                nextIndex = 1L;
                            }
                            nextIndexs.put(peer, nextIndex - 1); // 如果没有成功，就往前移动
                            log.warn("follower {} nextIndex not match, will resuce nextIndex and retry RPC append, nextIndex : [{}]", peer.getAddr(), nextIndex);
                            // 重来，直到成功。
                        }
                    }
                    end = System.currentTimeMillis(); // 直到超时
                }catch (Exception e){
                    log.warn(e.getMessage(), e);
                    return false;
                }
            }
            // 超时，复制失败
            return false;
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

    class ElectionTask implements Runnable {
        public void run() {
            if (status == NodeStatus.LEADER) {
                return;
            }
            long current = System.currentTimeMillis();
            // 基于RAFT的随机时间，解决冲突
            //1.7增加该类，企图将它和Random结合以克服所有的性能问题，该类继承自Random。
            //
            //Random用到了compareAndSet + synchronized来解决线程安全问题，虽然可以使用ThreadLocal<Random>来避免竞争，但是无法避免synchronized/compareAndSet带来的开销。考虑到性能还是建议替换使用ThreadLocalRandom（有3倍以上提升），这不是ThreadLocal包装后的Random，而是真正的使用ThreadLocal机制重新实现的Random。
            //
            //ThreadLocalRandom的主要实现细节：
            //
            //使用一个普通的long而不是使用Random中的AtomicLong作为seed
            //不能自己创建ThreadLocalRandom实例，因为它的构造函数是私有的，可以使用它的静态工厂ThreadLocalRandom.current()
            //它是CPU缓存感知式的，使用8个long虚拟域来填充64位L1高速缓存行
            electionTime = electionTime + ThreadLocalRandom.current().nextInt(50);

            // 如果没有到选举的时间点
            if (current - preElectionTime < electionTime) {
                return;
            }
            status = NodeStatus.CANDIDATE;
            log.error("node {} will become CANDIDATE and start election leader, current term : [{}], lastEntry : [{}]", peerSet.getSelf(),
                    currentTerm, logModule.getLast());
            preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;
            currentTerm = currentTerm + 1;
            // 把票投给自己，推荐自己当leader
            votedFor = peerSet.getSelf().getAddr();
            // 集群中其他成员
            List<Peer> peers = peerSet.getPeersWithOutSelf();
            ArrayList<Future<RvoteResult>> futureArrayList = new ArrayList<>();
            log.info("peerList size :{}, peer list content : {}", peers.size(), peers);

            // 发送投票请求
            for (Peer peer : peers) {
                futureArrayList.add(RaftThreadPool.submit(() -> {
                    long lastTerm = 0L; // 初始化任期为0
                    LogEntry last = logModule.getLast();
                    if (last != null) {
                        lastTerm = last.getTerm(); //更新为实际的上一轮任期
                    }
                    RvoteParam param = RvoteParam.builder().
                            term(currentTerm).
                            candidateId(peerSet.getSelf().getAddr()).
                            lastLogIndex(LongConvert.convert(logModule.getLastIndex())).
                            lastLogTerm(lastTerm).
                            build();
                    // 包装请求
                    Request request = Request.builder().
                            cmd(Request.R_VOTE)
                            .obj(param)
                            .url(peer.getAddr())
                            .build();
                    try {
                        return getRpcClient().<RvoteResult>send(request);
                    } catch (RaftRemotingException e) {
                        log.error("ElectionTask RPC Fail, URL : " + request.getUrl());
                        return null;
                    }
                }));
            }
            AtomicInteger success2 = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(futureArrayList.size());

            log.info("futureArrayList.size() : {}", futureArrayList.size());
            for (Future<RvoteResult> future : futureArrayList) {
                RaftThreadPool.submit(() -> {
                    try {
                        // future异步计算通过get获取结果
                        RvoteResult result = future.get(3000, MILLISECONDS);
                        if (result == null) {
                            return -1;
                        }
                        boolean isVoteGranted = result.isVoteGranted();
                        if (isVoteGranted) { // 如果这个成员给节点投了赞成票
                            success2.incrementAndGet();
                        } else {
                            long resTerm = result.getTerm();
                            if (resTerm >= currentTerm) { // 如果请求成为leader的节点的任期号比我小，那我在这种情况下要投反对票
                                currentTerm = resTerm;
                            }
                        }
                        return 0;
                    } catch (Exception e) {
                        log.error("future.get exception, e : ", e);
                        return -1;
                    } finally {
                        latch.countDown();
                    }
                });
            }
            try {
                // 等待CountDownLatch里面的执行完，才会继续往下执行
                latch.await(3500, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("InterruptedException By Master election Task");
            }
            int success = success2.get(); // 得到给自己投票的票数
            log.info("node {} maybe leader, success count = {}, status : {}", peerSet.getSelf(), success, NodeStatus.Enum.value(status));
            // 如果投票期间，有其他服务器成功当选为leader，并且给我这个节点发送了appendEntry，那么我这个节点就可能从候选者变成follwer，这时，就应该停止
            if (status == NodeStatus.FOLLOWER) {
                return;
            }
            if (success >= peers.size() / 2) {
                log.warn("node {} become leader ", peerSet.getSelf());
                status = NodeStatus.LEADER;
                peerSet.setLeader(peerSet.getSelf());
                votedFor = "";
                becomeLeaderToDoThing();
            }else {
                votedFor = ""; // 否则就重新开始选举
            }
            preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;
        }
    }

    /**
     * 变成领导之后要做的事情
     * 1、将所有nextIndex值初始化为其日志中最后一个索引之后的索引（如果跟随者的日志和领导者的日志不一致，则在下一个AppendEntries RPC中，一致性检查将失败
     * ，拒绝后，领导者递减nextIndex并重试AppendEntries RPC。最终nextIndex将达到领导者和跟随者日志匹配的点。）
     */
    private void becomeLeaderToDoThing() {
        // 对于每一个服务器，下一个需要发送给他的日志条目的索引号
        nextIndexs = new ConcurrentHashMap<>();
        // 对于每一个服务器，已经复制给他的最高的日志索引号。
        matchIndexs = new ConcurrentHashMap<>();
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            nextIndexs.put(peer, logModule.getLastIndex() + 1);
            matchIndexs.put(peer, 0L); // 初始这个leader节点还没给他复制上。所以是0。然后寻找到最后一个和leader节点匹配得索引位置。更新这个值。
        }
        // 写个空日志，发送心跳，告诉别的服务器我是新的leader。停止他们接下来得选举
        LogEntry logEntry = LogEntry.builder()
                .command(null)
                .term(currentTerm)
                .build();
        logModule.write(logEntry);
        log.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());
        final AtomicInteger success = new AtomicInteger(0);
        List<Future<Boolean>> futureList = new ArrayList<>();
        int count = 0;
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            count++;
            // 向follower节点发送心跳信息，成功确认自己是leader节点之后。同时也就更新了matchIndexs
            futureList.add(replication(peer, logEntry));
        }

        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();

        getRPCAppendResult(futureList, latch, resultList);
        try {
            latch.await(4000, MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet(); //成功的个数自增1
            }
        }

        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values());
        int median = 0;
        if (matchIndexList.size() >= 2) {
            Collections.sort(matchIndexList);
            median = matchIndexList.size() / 2;
        }
        Long N = matchIndexList.get(median);
        if (N > commitIndex) {
            LogEntry entry = logModule.read(N);
            if (entry != null && entry.getTerm() == currentTerm) {
                commitIndex = N;
            }
        }

        // 复制响应一半以上成功
        if (success.get() >= (count / 2)) {
            commitIndex = logEntry.getIndex();
            getStateMachine().apply(logEntry); // 将该leader节点中的日志进行提交，如果提交得日志中command为null，则直接返回到这里
            lastApplied = commitIndex; // 最后被应用到此leader状态机上的日志条目索引
            log.info("success apply local state machine, logEntry info : {}", logEntry);
        }else {
            logModule.removeOnStartIndex(logEntry.getIndex());
            log.warn("fail apply local state machine, logEntry info : {}", logEntry);

            // 无法提交空日志，让出领导者位置
            log.warn("node {} becomeLeaderToDoThing fail ", peerSet.getSelf());
            status = NodeStatus.FOLLOWER;
            peerSet.setLeader(null);
            votedFor = "";
        }
    }


    // 对leader中复制失败的要重新复制
    class ReplicationFailQueueConsumer implements Runnable {
//        // 间隔时间（1分钟）
//        long intervalTime = 1000 * 60;
//        @Override
        public void run() {
//            while (running) {
//                try {
//                    ReplicationFailModel model = replicationFailQueue.poll(1000, MILLISECONDS);
//                    if (model == null) {
//                        return;
//                    }
//                    if (status != NodeStatus.LEADER) {
//                        replicationFailQueue.clear();
//                        continue;
//                    }
//                    log.warn("replication Fail Queue Consumer take a task, will be retry replication, content detail : [{}]", model.logEntry);
//                    long offerTime = model.offerTime;
//                    // 如果长时间没有复制成功
//                    if (System.currentTimeMillis() - offerTime > intervalTime) {
//                        log.warn("replication Fail event Queue maybe full or handler slow");
//                    }
//                    Callable callable = model.callable;
//                    Future<Boolean> future = RaftThreadPool.submit(callable);
//                    Boolean r = future.get(3000, MILLISECONDS);
//                    if (r) {
//                        tryApplyStateMachine(model);
//                    }
//                } catch (InterruptedException e) {
//                    // ignore
//                } catch (ExecutionException | TimeoutException e) {
//                    log.warn(e.getMessage());
//                }
//            }
        }
    }
}
