package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.LifeCycle;

public interface LogModule extends LifeCycle {
    void write(logEntry logEntry);
}
