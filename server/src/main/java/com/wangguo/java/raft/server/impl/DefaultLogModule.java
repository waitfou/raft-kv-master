package com.wangguo.java.raft.server.impl;

import com.alibaba.fastjson.JSON;
import com.wangguo.java.raft.common.entity.LogEntry;
import com.wangguo.java.raft.server.LogModule;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import sun.rmi.runtime.Log;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 默认的日志实现，日志模块不关心key， 只关心index.
 */
@Getter
@Setter
@Slf4j
public class DefaultLogModule implements LogModule {
    public String dbDir;
    public String logsDir;
    private RocksDB logDb;

    //"LAST_INDEX_KEY".getBytes()表示把字符串转换成字节流
    public final static byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();
    /**
     * java.util.concurrent.locks包提供的ReentrantLock用于替代synchronized加锁
     * 因为synchronized是Java语言层面提供的语法，所以我们不需要考虑异常，而ReentrantLock是
     * Java代码实现的锁，我们就必须先获取锁，然后在finally中正确释放锁。
     * 顾名思义，ReentrantLock是可重入锁，它和synchronized一样，一个线程可以多次获取同一个锁。
     */
    private ReentrantLock lock = new ReentrantLock();
    private DefaultLogModule(){
        //目录地址
        if(dbDir == null){
            //从运行的程序中获取当前程序使用的端口
            dbDir = "./rocksDB-raft/" + System.getProperty("serverPort");
        }
        //日志地址
        if(logsDir == null){
            logsDir = dbDir + "/logModule";
        }
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);

        File file = new File(logsDir);
        boolean success = false;
        if(!file.exists()){ //如果日志文件不存在，那么就创建一个。适用于新创建的节点还没有日志模块的时候
            success = file.mkdir();
        }
        if(success){
            log.warn("make a new dir : " + logsDir);
        }
        try{
            System.out.println("test3");
            logDb = RocksDB.open(options, logsDir);
            System.out.println(logDb);
            System.out.println("test4");
        } catch (RocksDBException e) {
            log.info(e.getMessage());
        }
    }

    public static DefaultLogModule getInstance(){
        return DefaultLogsLazyHolder.INSTANCE;
    }

    @Override
    public void init() throws Throwable {

    }

    @Override
    public void destroy() throws Throwable {
        logDb.close();
        log.info("destory success");
    }

    private static class DefaultLogsLazyHolder{
        private static final DefaultLogModule INSTANCE = new DefaultLogModule();
    }

    /**
     * 写日志
     */
    @Override
    public void write(LogEntry logEntry){
        boolean success = false;
        boolean result;
        try{
            // lock()方法是一个无条件的锁，与synchronize意思差不多，直接去获取锁。
            // 成功了就ok了，失败了就进入阻塞等待了。不同的是lock锁是可重入锁。
            result = lock.tryLock(3000, MILLISECONDS);
            if(!result){ //如果获取锁失败
                throw new RuntimeException("write fail, tryLock fail");
            }
            //设置日志实体的索引值
            logEntry.setIndex(getLastIndex() + 1);
            //在logDb中是基于日志索引对日志进行检索的（键为日志实体索引，值为日志实体本身）
            //JSON.toJSONBytes把对象序列化为Byte[]数组
            logDb.put(logEntry.getIndex().toString().getBytes(), JSON.toJSONBytes(logEntry));
            success = true;
            log.info("DefaultLogModule write rocksDB success, LogEntry info : [{}]", logEntry);
        } catch (RocksDBException | InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            if(success){
                 // 如果成功，更新当前follower节点的最后一个索引
                updateLastIndex(logEntry.getIndex());
            }
            lock.unlock();//释放锁
        }
    }

    /**
     * 读取日志
     * @param index
     * @return
     */
    @Override
    public LogEntry read(Long index) {
        try{
            byte[] result = logDb.get(convert(index));
            if(result == null){
                return null;//没有对应的日志
            }
            //把JSON字节流转换成Object（在这里是LogEntry实体）
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从指定索引开始将后面的每一个日志移除
     * @param startIndex
     */
    @Override
    public void removeOnStartIndex(Long startIndex) {
        boolean success = false;
        int count = 0;
        boolean tryLock;
        try{
            tryLock = lock.tryLock(3000, MILLISECONDS);
            if(!tryLock){
                throw new RuntimeException("tryLock fail, removeOnStartIndex fail");
            }
            for(long i = startIndex; i <= getLastIndex(); i++){
                logDb.delete(String.valueOf(i).getBytes());
                count++;
            }
            success = true;
            log.warn("rocksDB removeOnStartIndex success,count={} startIndex={}, lastIndex={}", count, startIndex, getLastIndex());
        } catch (InterruptedException | RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            if (success) {
                updateLastIndex(getLastIndex() - count);
                lock.unlock();
            }
        }
    }

    /**
     * 获取最后一个索引
     * @return
     */
    @Override
    public Long getLastIndex() {
        byte[] lastIndex;
        try{
            lastIndex = logDb.get(LAST_INDEX_KEY);
            if(lastIndex == null){
                lastIndex = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return Long.valueOf(new String(lastIndex));
    }

    /**
     * 把键转换成字节流，因为RocksDB的键和值是存储的键值流
     * @param key
     * @return
     */
    public byte[] convert(Long key){
        return key.toString().getBytes();
    }

    /**
     * 获取最后一个日志实体
     * @return
     */
    @Override
    public LogEntry getLast() {
        try{
            byte[] result = logDb.get(convert(getLastIndex()));
            if(result == null){
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 更新最后一个索引
     * @param index
     */
    private void updateLastIndex(Long index){
        try{
            logDb.put(LAST_INDEX_KEY, index.toString().getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

}
