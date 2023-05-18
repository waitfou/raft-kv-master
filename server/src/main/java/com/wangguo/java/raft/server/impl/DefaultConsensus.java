package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.entity.*;
import com.wangguo.java.raft.server.Consensus;
import io.netty.util.internal.StringUtil;
import lombok.extern.java.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;



public class DefaultConsensus implements Consensus {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsensus.class);
    public final DefaultNode node;

    // RentrantLock是可重入的互斥锁
    public final ReentrantLock appendLock = new ReentrantLock();
    public final ReentrantLock voteLock = new ReentrantLock();
    public DefaultConsensus(DefaultNode node){
        this.node = node;
    }


    // 初期来自服务器的请求投票实现
    @Override
    public RvoteResult requestVote(RvoteParam param) {
        try {
            RvoteResult.Builder builder = RvoteResult.newBuilder();
            // 如果加锁失败，那么拒绝投票
            if (!voteLock.tryLock()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // 如果对方任期没有自己新，那么拒绝给它投票
            if (param.getTerm() < node.getCurrentTerm()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }
            LOGGER.info("node {} current vote for [{}], param candidateId : {}", node.peerSet.getSelf(), node.getVotedFor(), param.getCandidateId());
            LOGGER.info("node {} current term {}, peer term : {}", node.peerSet.getSelf(), node.getCurrentTerm(), param.getTerm());

            if((StringUtil.isNullOrEmpty(node.getVotedFor()) || node.getVotedFor().equals(param.getCandidateId()))) {
                if (node.getLogModule().getLast() != null) {
                    // 如果对方没有自己的任期新，那么拒绝给他投票
                    if (node.getLogModule().getLast().getTerm() > param.getLastLogTerm()) {
                        return RvoteResult.fail();
                    }
                    // ???
                    if (node.getLogModule().getLastIndex() > param.getLastLogIndex()) {
                        return RvoteResult.fail();
                    }
                }
                // 切换自己为follower
                node.status = NodeStatus.FOLLOWER;
                // ???
                node.peerSet.setLeader(new Peer(param.getCandidateId()));
                node.setCurrentTerm(param.getTerm());
                node.setVotedFor(param.getServerId());

                // 返回成功
                return builder.term(node.currentTerm).voteGranted(true).build();
            }
            return builder.term(node.currentTerm).voteGranted(false).build();
        } finally {
            voteLock.unlock();
        }
    }

    /**
     * 根据领导者发送过来的追加日志信息，向该节点追加日志
     * @param param
     * @return
     */
    @Override
    public AentryResult appendEntries(AentryParam param) { // 处理心跳（日志实体为空），或者追加日志
        // 用来保存追加日志的结果
        AentryResult result = AentryResult.fail();
        try{
            // 首先获取锁
            if(!appendLock.tryLock()){
                return result;
            }
            result.setTerm(node.getCurrentTerm());
            // 不够格，要追加的日志的任期号，必须比当前节点里面的任期号大
            if(param.getTerm() < node.getCurrentTerm()){
                return result;
            }
            node.preHeartBeatTime = System.currentTimeMillis();
            node.preElectionTime = System.currentTimeMillis();
            //接收到心跳，再次同意对方是领导者
            node.peerSet.setLeader((new Peer(param.getLeaderId())));

            // 如果领导者的任期号至少与候选者的当前任期一样大，则候选者承认领导者是合法的并返回到跟随者状态
            // （为什么说是返回，因为当前节点可能现在是候选者节点正在投票选举）。
            if (param.getTerm() >= node.getCurrentTerm()){
                LOGGER.debug("node {} become FOLLOWER, currentTerm : {}, param Term : {}, param serverId = {}",
                        node.peerSet.getSelf(), node.currentTerm, param.getTerm(), param.getServerId());
                // 自己继续确认自己是Follower节点
                node.status = NodeStatus.FOLLOWER;
            }
            //使用领导者的任期号
            node.setCurrentTerm(param.getTerm());

            //心跳，同时完成follower节点中日志的提交
            if(param.getEntries() == null || param.getEntries().length == 0){ //如果领导者发送过来的信息中日字体中没有信息，那么就是心跳信息。
                LOGGER.info("node {} append heartbeat success, he's term : {}, my term : {}",
                        param.getLeaderId(), param.getTerm(), node.getCurrentTerm());

                long nextCommit = node.getCommitIndex() + 1;

                // 借助leader节点发送过来的心跳信息，将follower节点的提交的日志更新为和leader一样，因为leader的提交是在复制给follower节点之后
                // 发生的，那么提交了的日志，一定已经复制给了超过一半的follower节点
                if (param.getLeaderCommit() > node.getCommitIndex()) {
                    // 之所以取得最小值，是因为有个人leader中提交了的日志，没有复制到该follower节点。
                    int commitIndex = (int)Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                    node.setCommitIndex(commitIndex);
                    node.setLastApplied(commitIndex);
                }
                while (nextCommit <= node.getCommitIndex()) {
                    // 正式提交follower节点中的未和leader节点提交状态保持一致的日志实体，应用到状态机之后才是真正地提交了
                    node.stateMachine.apply(node.logModule.read(nextCommit));
                    nextCommit++;
                }
                return AentryResult.newBuilder().term(node.getCurrentTerm()).success(true).build();
            }

            //不是心跳，那么就是要复制真实日志
            if(node.getLogModule().getLastIndex()!=0 && param.getPreLogIndex() != 0){
                LogEntry logEntry;
                if((logEntry = node.getLogModule().read(param.getPreLogIndex())) != null){
                    //看领导者节点中和该节点中的该索引日志的任期号是否一致。不一致就是不匹配吗，返回false
                    if (logEntry.getTerm() != param.getPreLogTerm()){
                        return result;
                    }
                } else{
                    // index 不对，该follower节点这个位置没有日志，需要递减nextIndex重试。
                    return result;
                }
            }
            //直到找到应该和leader节点保存一致的追加起点
            //如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的。（保证和Leader节点的强一致性）
            //param参数中没存储当前要复制的日志索引，但是可以通过param.getPreLogIndex() + 1得到
            LogEntry existLog = node.getLogModule().read(((param.getPreLogIndex() + 1)));
            if (existLog != null && existLog.getTerm() != param.getEntries()[0].getTerm()){
                // 移除该fllower节点该位置之后的所有节点，方便复制日志，保存强一致性
                node.getLogModule().removeOnStartIndex(param.getPreLogIndex() + 1);
            }else if(existLog != null){
                // 该索引位置已经有日志了，不能重复写入。
                result.setSuccess(true);
                return result;
            }

            // 该索引位置没有日志，那么就写入日志（此时会写入索引leader节点已经提交的日志）
            for (LogEntry entry : param.getEntries()){
                node.getLogModule().write(entry);
                result.setSuccess(true);
            }

            // 日志追加日志，下一个需要提交的日志的索引（如果有）就是上一次已经提交的日志后移一位。
            long nextCommit = node.getCommitIndex() + 1;

            // 如果leaderCommit > commitIndex, 那么令commitIndex 等于 LeaderCommit和新日志条目索引值中较小的一个。
            // 因为follow节点中索引在leader已经提交的索引日志之后的日志在保证和leader强一致性的过程中要被抛弃
            if (param.getLeaderCommit() > node.getCommitIndex()){
                int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                node.setCommitIndex(commitIndex);
                node.setLastApplied(commitIndex);
            }

            while (nextCommit <= node.getCommitIndex()) { // 提交日志
                node.stateMachine.apply(node.logModule.read(nextCommit));
                nextCommit++;
            }
            result.setTerm(node.getCurrentTerm());
            node.status = NodeStatus.FOLLOWER;
            return result;
        } finally {
            appendLock.unlock();
        }
    }
}
