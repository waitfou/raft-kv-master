package com.wangguo.java.raft.server.changes;

import com.wangguo.java.raft.common.entity.Peer;

/**
 * 集群配置变更接口
 */
public interface ClusterMembershipChanges {
    /**
     * 添加节点
     */
    Result addPeer(Peer newPeer);
    /**
     * 删除节点
     */
    Result removePeer(Peer oldPeer);
}
