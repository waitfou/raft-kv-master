package com.wangguo.java.raft.server.current;


import com.alipay.remoting.NamedThreadFactory;

import java.util.concurrent.*;

/**
 * 线程池
 */
public class RaftThreadPool {
    private static int cup = Runtime.getRuntime().availableProcessors();//获取可用的处理器数
    //如果是IO密集型应用，则线程池大小设置为2N+1(或者是2N)
    private static int maxPoolSize = cup * 2;
    private static final int queueSize = 1024;
    private static final long keepTime = 1000 * 60;
    private static TimeUnit keepTimeUnit = TimeUnit.MILLISECONDS;

    private static ScheduledExecutorService ss = getScheduled();

    private static ThreadPoolExecutor te = getThreadPool();
    private static ThreadPoolExecutor getThreadPool() {
        return new RaftThreadPoolExecutor(
                cup,
                maxPoolSize,
                keepTime,
                keepTimeUnit,
                new LinkedBlockingDeque<>(queueSize),
                new NameThreadFactory());
    }
    private static ScheduledExecutorService getScheduled(){
        return new ScheduledThreadPoolExecutor(cup, new NameThreadFactory());
    }

    public static void scheduleAtFixedRate(Runnable r, long initDelay, long delay){
        ss.scheduleAtFixedRate(r, initDelay, delay, TimeUnit.MILLISECONDS);
    }

    public static void scheduleWithFixedDelay(Runnable r, long delay){
        ss.scheduleWithFixedDelay(r, 0, delay, TimeUnit.MILLISECONDS);
    }
    public static void execute(Runnable r){
        te.execute(r);
    }
    public static void execute(Runnable r, boolean sync){
        if(sync){
            r.run();
        }else{
            te.execute(r);
        }
    }
    /**
     * 创建线程的工厂
     */
    static class NameThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            Thread t = new RaftThread("Raft thread", r);
            //标记线程为守护线程或者用户线程
            t.setDaemon(true);
            //设置线程的优先级
            t.setPriority(5);
            return t;
        }
    }
    //在线程池中，如果我们需要返回结果则可以调用submit方法，如果需要执行结果的话则可以实现callable，实现Runnable的线程是没有返回值的
    public static <T> Future<T> submit(Callable r){
        return te.submit(r);
    }
}
