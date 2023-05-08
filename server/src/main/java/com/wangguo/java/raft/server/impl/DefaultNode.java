package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.entity.*;
import com.wangguo.java.raft.server.Node;
import com.wangguo.java.raft.server.changes.ClusterMembershipChanges;
import com.wangguo.java.raft.server.changes.Result;
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
        super();
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

    /**
     * 默认节点初始化时的一些操作
     * @throws Throwable
     */
    @Override
    public void init() throws Throwable {
        //设置节点的运行状态为true
        running = true;
        rpc
    }

    @Override
    public void destroy() throws Throwable {

    }

    @Override
    public void setConfig(NodeConfig config) {

    }

    @Override
    public Result addPeer(Peer newPeer) {
        return null;
    }

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
    public AentryResult handlerAppendEntries(AentryParam param){}
}
