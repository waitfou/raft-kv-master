package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.LifeCycle;
import com.wangguo.java.raft.common.rpc.Request;
import com.wangguo.java.raft.common.rpc.Response;

/**
 * 对服务端的rpc进行管理
 */
public interface RpcService extends LifeCycle {
    /**
     * 对请求进行处理，并且以Response方式返回
     * @param request
     * @return
     */
    Response<?> handlerRequest(Request request);//对请求进行处理
}
