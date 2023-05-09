package com.wangguo.java.raft.server.current;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftThread extends Thread{
    /**
     * 使用指定类初始化日志对象
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftThread.class);
    /**
     * t表示线程名，e表示异常类型
     */
    private static final UncaughtExceptionHandler uncaughtExceptionHandler = (t, e)
        -> LOGGER.warn("Excetion occurred from thread{}", t.getName(), e);
    public RaftThread(String threadName, Runnable r){
        super(r, threadName);
        /**
         * 设置程序因为没有捕获的异常中止时的处理程序
         */
        setUncaughtExceptionHandler(uncaughtExceptionHandler);
    }
}
