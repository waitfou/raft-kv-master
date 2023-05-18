package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.common.entity.LogEntry;
import com.wangguo.java.raft.common.entity.NodeStatus;
import com.wangguo.java.raft.common.entity.Peer;
import com.wangguo.java.raft.common.rpc.Request;
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

    // 必须指定是哪个节点调用的这个集群节点改变类
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
        // 如果集群中已经包含了该节点，那么直接返回。
        if (node.peerSet.getPeersWithOutSelf().contains(newPeer)) {
            return new Result();
        }
        node.peerSet.getPeersWithOutSelf().add(newPeer);
        if (node.status == NodeStatus.LEADER) {
            // 表示新加入的节点。下一个需要加入的日志的索引为1
            node.nextIndexs.put(newPeer, 0L);
            // 表示新加入的节点。最后一个已经复制给他的日志是
            node.matchIndexs.put(newPeer, 0L);
            for (long i = 0; i < node.logModule.getLastIndex(); i++){
                // 把所有的日志复制给这个新节点
                LogEntry entry = node.logModule.read(i);
                if (entry != null){
                    node.replication(newPeer, entry);
                }
            }
            for (Peer ignore : node.peerSet.getPeersWithOutSelf()) {
                // 把新加入一个节点的信息同步给别的节点
                Request request = Request.builder()
                        .cmd(Request.CHANGE_CONFIG_ADD)
                        .url(newPeer.getAddr())
                        .obj(newPeer)
                        .build();
                Result result = node.rpcClient.send(request);
            }
        }
        return new Result();
    }

    @Override
    public Result removePeer(Peer oldPeer) {
        return null;
    }
}
