package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.entity.*;
import com.wangguo.java.raft.server.Consensus;
import lombok.extern.java.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;



public class DefaultConsensus implements Consensus {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsensus.class);
    public final DefaultNode node;

    public final ReentrantLock appendLock = new ReentrantLock();
    public DefaultConsensus(DefaultNode node){
        this.node = node;
    }


    /**
     * 更具领导者发送过来的追加日志信息，向该节点追加日志
     * @param param
     * @return
     */
    @Override
    public AentryResult appendEntries(AentryParam param) {
        AentryResult result = AentryResult.fail();
        try{
            if(!appendLock.tryLock()){
                return result;
            }
            result.setTerm(node.getCurrentTerm());
            // 不够格
            if(param.getTerm() < node.getCurrentTerm()){
                return result;
            }
            node.preHeartBeatTime = System.currentTimeMillis();
            node.preElectionTime = System.currentTimeMillis();
            //接收到心跳，再次同意对方是领导者
            node.peerSet.setLeader((new Peer(param.getLeaderId())));

            //够格
            if (param.getTerm() >= node.getCurrentTerm()){
                LOGGER.debug("node {} become FOLLOWER, currentTerm : {}, param Term : {}, param serverId = {}",
                        node.peerSet.getSelf(), node.currentTerm, param.getTerm(), param.getServerId());
                // 自己继续确认自己是Follower节点
                node.status = NodeStatus.FOLLOWER;
            }
            //使用领导者的任期号
            node.setCurrentTerm(param.getTerm());

            //心跳
            if(param.getEntries() == null || param.getEntries().length == 0){ //如果领导者发送过来的信息中日字体中没有信息，那么就是心跳信息。
                LOGGER.info("node {} append heartbeat success, he's term : {}, my term : {}",
                        param.getLeaderId(), param.getTerm(), node.getCurrentTerm());

            }
            //不是心跳，那么就是要复制日志
            if(node.getLogModule().getLastIndex()!=0 && param.getPreLogIndex() != 0){
                LogEntry logEntry;
                if((logEntry = node.getLogModule().read(param.getPreLogIndex())) != null){
                    //看领导者节点中和该节点中的该索引日志的任期号是否一致。不一致就是不匹配吗，返回false
                    if (logEntry.getTerm() != param.getPreLogTerm()){
                        return result;
                    }
                } else{
                    // index 不对，需要递减nextIndex重试。
                    return result;
                }
            }
            //如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的。（保证和Leader节点的强一致性）
            //param参数中没存储当前要复制的日志索引，但是可以通过param.getPreLogIndex() + 1得到
            LogEntry existLog = node.getLogModule().read(((param.getPreLogIndex() + 1)));
            if (existLog != null && existLog.getTerm() != param.getEntries()[0].getTerm()){
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

            // 下一个需要提交的日志的索引（如果有）
            long nextCommit = node.getCommitIndex() + 1;

            // 如果leaderCommit > commitIndex, 那么令commitIndex 等于 LeaderCommit和新日志条目索引值中较小的一个。
            // 因为follow节点中索引在leader已经提交的索引日志之后的日志在保证和leader强一致性的过程中要被抛弃
            if (param.getLeaderCommit() > node.getCommitIndex()){
                int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
            }
        }
    }
}
