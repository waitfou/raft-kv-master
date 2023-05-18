package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.LifeCycle;
import com.wangguo.java.raft.common.entity.LogEntry;

/**
 * 状态机接口
 */
public interface StateMachine extends LifeCycle {
    /**
     * 将数据应用到状态机, 也就是提交到状态机
     */
    void apply(LogEntry logEntry);
    // 从状态机中获取日志实体
    LogEntry get(String key);
    // 从状态机中获取日志信息
    String getString(String key);

    void setString(String key, String value);
    // 从状态机中删除日志信息
    void delString(String... key);
}
