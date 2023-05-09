package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.LifeCycle;
import com.wangguo.java.raft.common.entity.*;

public interface Node extends LifeCycle {
    /**
     * 设置配置文件
     * @param config
     */
    void setConfig(NodeConfig config);

    /**
     * 处理请求投票RPC
     * @param param
     * @return
     */
    RvoteResult handlerRequestVote(RvoteParam param);

    /**
     * 处理附加日志请求
     * @param param
     * @return
     */
    AentryResult handlerAppendEntries(AentryParam param);

    /**
     * 处理客户端请求
     * @param request
     * @return
     */
    ClientKVAck handlerClientRequest(ClientKVReq request);

    /**
     * 如果不是Leader节点，那么节点接收到的消息都要转发给leader节点
     * @param request
     * @return
     */
    ClientKVAck redirect(ClientKVReq request);
}
