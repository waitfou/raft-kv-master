package com.wangguo.java.raft.server.impl;

import com.alibaba.fastjson.JSON;
import com.wangguo.java.raft.common.entity.Command;
import com.wangguo.java.raft.common.entity.LogEntry;
import com.wangguo.java.raft.server.StateMachine;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

/**
 * 默认状态机
 */
@Slf4j
public class DefaultStateMachine implements StateMachine {
    public String dbDir;
    public String stateMachineDir;

    // 用RocksDB存放日志
    public RocksDB machineDb;

    private DefaultStateMachine() {
        // 该状态机的地址
        dbDir = "./rocksDB-raft/" + System.getProperty("serverPort");
        stateMachineDir = dbDir + "/stateMachine";
        RocksDB.loadLibrary();

        File file = new File(stateMachineDir);
        boolean success = false;
        if (!file.exists()) {
            /**
             * 注意：file.mkdirs与file.mkdir的区别
             * file.mkdir() 如果你想在已经存在的文件夹下建立新的文件夹，
             * 就可以用此方法。此方法不能在不存在的文件夹下建立新的文件夹。
             * 假如想建立名字是”2019-03-16”文件夹，那么它的父文件夹必须存在。
             *
             * file.mkdirs() 如果你想根据File里的路径名建立文件夹
             * （当你不知道此文件夹是否存在，也不知道父文件夹存在），
             * 就可用此方法，它建立文件夹的原则是：如果父文件夹不存在并且最后
             * 一级子文件夹不存在，它就自动新建所有路经里写的文件夹；
             * 如果父文件夹存在，它就直接在已经存在的父文件夹下新建子文件夹
             */
            success = file.mkdirs();
        }
        if (success) {
            log.warn("make a new dir:" + stateMachineDir);
        }
        Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            System.out.println("test1");
            machineDb = RocksDB.open(options, stateMachineDir);
            System.out.println("test2");
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static DefaultStateMachine getInstance() {
        return DefaultStateMachineLazyHolder.INSTANCE;
    }

    private static class DefaultStateMachineLazyHolder {
        private static final DefaultStateMachine INSTANCE = new DefaultStateMachine();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public void init() throws Throwable {

    }

    @Override
    public void destroy() throws Throwable {
        machineDb.close();
        log.info("destory success");
    }

    @Override
    public void apply(LogEntry logEntry) { // 将日志正式提交到状态机上
        try {
            Command command = logEntry.getCommand();
            if (command == null) {
                log.warn("insert no-op log, logEntry={}", logEntry);
                return;
            }
            String key = command.getKey();
            // 将该条日志信息存放到该状态机的RocksDB中
            machineDb.put(key.getBytes(), JSON.toJSONBytes(logEntry));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LogEntry get(String key) {
        try {
            byte[] result = machineDb.get(key.getBytes());
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getString(String key) {
        try {
            byte[] bytes = machineDb.get(key.getBytes());
            if (bytes != null) {
                return new String(bytes);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return "";
    }

    @Override
    public void setString(String key, String value) {
        try {
            machineDb.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delString(String... key) { //String... 表示字符串参数个数不固定，也就是叫做可变长参数
        try {
            for (String s : key) {
                machineDb.delete(s.getBytes());
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
