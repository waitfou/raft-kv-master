package com.wangguo.java.raft.common.rpc;

import com.alipay.remoting.exception.RemotingException;
import com.wangguo.java.raft.common.RaftRemotingException;
import lombok.extern.slf4j.Slf4j;


import java.util.concurrent.TimeUnit;


/**
 * 客户端的默认RPC状态
 */
@Slf4j
public class DefaultRpcClient implements RpcClient {
    private final static com.alipay.remoting.rpc.RpcClient CLIENT = new com.alipay.remoting.rpc.RpcClient();

    /**
     * 不提供超时参数时
     * @param request 参数
     * @return
     * @param <R>
     */
    @Override
    public <R> R send(Request request) {
        return send(request, (int) TimeUnit.SECONDS.toMillis(5));
    }

    /**
     * 提供超时参数
     * @param request
     * @param timeout
     * @return
     * @param <R>
     */
    @Override
    public <R> R send(Request request, int timeout) {
        Response<R> result;
        try{
            //invokeSync是同步调用
            result = (Response<R>) CLIENT.invokeSync(request.getUrl(), request, timeout);
            return result.getResult();
        } catch (RemotingException e) {
            throw new RaftRemotingException("rpc RaftRemotingException ", e);
        } catch (InterruptedException e) {
            // ignore
        }
        return null;
    }

    @Override
    public void init(){
        /**
         * 客户端初始化
         */
        CLIENT.init();
    }

    @Override
    public void destroy(){
        /**
         * 客户端销毁
         */
        CLIENT.shutdown();
        log.info("destory success");
    }
}
