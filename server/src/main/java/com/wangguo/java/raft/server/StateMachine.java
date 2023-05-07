package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.LifeCycle;
import com.wangguo.java.raft.common.entity.LogEntry;

/**
 * 状态机接口
 */
public interface StateMachine extends LifeCycle {
    /**
     * 将数据应用到状态机
     */
    void apply(LogEntry logEntry);
    LogEntry get(String key);
    String getString(String key);
    void setString(String key, String value);
    void delString(String... key);
}
