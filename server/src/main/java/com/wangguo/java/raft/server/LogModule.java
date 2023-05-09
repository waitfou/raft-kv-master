package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.LifeCycle;
import com.wangguo.java.raft.common.entity.LogEntry;

public interface LogModule extends LifeCycle {
    void write(LogEntry logEntry);
    LogEntry read(Long index);
    void removeOnStartIndex(Long startIndex);
    LogEntry getLast();
    Long getLastIndex();
}
