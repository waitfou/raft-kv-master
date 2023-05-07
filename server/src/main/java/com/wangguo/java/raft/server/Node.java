package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.LifeCycle;
import com.wangguo.java.raft.common.entity.NodeConfig;

public interface Node extends LifeCycle {
    /**
     * 设置配置文件
     */
    void setConfig(NodeConfig config);

}
