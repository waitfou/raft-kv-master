package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.server.LogModule;
import org.rocksdb.RocksDB;

/**
 * 默认的日志实现，日志模块不关心key， 只关心index.
 */
public class DefaultLogModule implements LogModule {
    public String dbDir;
    public String logsDir;
    private RocksDB logDb;

}
