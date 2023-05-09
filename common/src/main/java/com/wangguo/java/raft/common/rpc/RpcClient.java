package com.wangguo.java.raft.common.rpc;

import com.wangguo.java.raft.common.LifeCycle;

public interface RpcClient extends LifeCycle {
    /**
     * 客户端发送请求，并同步等待返回值
     * @param request 参数
     * @return
     * @param <R> 返回值类型
     */
    <R> R send(Request request);
    <R> R send(Request request, int timeout);
}
