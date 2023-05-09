package com.wangguo.java.raft.server.current;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RaftThreadPoolExecutor extends ThreadPoolExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftThreadPoolExecutor.class);
    /**
     * ThreadLocal类变量，ThreadLocal.withInitial是为其赋予初始值，每个线程访问的时候独享一份
     */
    private static final ThreadLocal<Long> COST_TIME_WATCH = ThreadLocal.withInitial(System::currentTimeMillis);

    /**
     * 线程池定义
     * @param corePoolSize 核心线程数
     * @param maximumPoolSize 线程池最大线程数
     * @param keepAliveTime 空闲线程存活时间
     * @param unit 时间单位
     * @param workQueue 工作队列
     * @param nameThreadFactory 创建线程的工厂
     */
    public RaftThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingDeque<Runnable> workQueue, RaftThreadPool.NameThreadFactory nameThreadFactory){
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, nameThreadFactory);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        COST_TIME_WATCH.get(); //返回当前的ThreadLocal变量的值
        LOGGER.debug("raft thread pool before Execute");
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        LOGGER.debug("raft thread pool after Execute, cost time:{}", System.currentTimeMillis()-COST_TIME_WATCH.get());
    }

    @Override
    protected void terminated() {
        /**
         * 打印出线程池中的状态
         */
        LOGGER.info("active count:{}, queueSize:{},poolSize:{}",getActiveCount(), getQueue().size(),getPoolSize());
    }
}
