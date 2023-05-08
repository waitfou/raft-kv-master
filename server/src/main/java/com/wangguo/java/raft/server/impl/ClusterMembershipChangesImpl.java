package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.entity.Peer;
import com.wangguo.java.raft.server.changes.ClusterMembershipChanges;
import com.wangguo.java.raft.server.changes.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群配置变更接口的默认实现
 */
public class ClusterMembershipChangesImpl implements ClusterMembershipChanges {
    // 使用指定类初始化日志对象，在日志输出的时候。可以打印出日志信息所在类
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMembershipChangesImpl.class);
    private final DefaultNode node;

    public ClusterMembershipChangesImpl(DefaultNode node){
        this.node = node;
    }

    /**
     * 必须是同步的，一次只能添加一个节点
     * @param newPeer
     * @return
     */
    @Override
    public synchronized Result addPeer(Peer newPeer) {
        if(node.peerSe)
    }

    @Override
    public Result removePeer(Peer oldPeer) {
        return null;
    }
}
