package com.wangguo.java.raft.common;

/**
 * 生命周期
 * */
public interface LifeCycle {
    void init() throws Throwable;
    void destroy() throws Throwable;
}
