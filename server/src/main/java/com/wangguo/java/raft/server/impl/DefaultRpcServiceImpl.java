package com.wangguo.java.raft.server.impl;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.RpcServer;
import com.wangguo.java.raft.common.entity.RvoteParam;
import com.wangguo.java.raft.common.rpc.Request;
import com.wangguo.java.raft.common.rpc.Response;
import com.wangguo.java.raft.server.RpcService;
import com.wangguo.java.raft.server.rpc.RaftUserProcessor;
import lombok.extern.slf4j.Slf4j;

/**
 * Raft Server
 */
@Slf4j
public class DefaultRpcServiceImpl implements RpcService {
    private final DefaultNode node;
    private final RpcServer rpcServer;

    public DefaultRpcServiceImpl(int port, DefaultNode node){
        rpcServer = new RpcServer(port, false, false);
        rpcServer.registerUserProcessor(new RaftUserProcessor<Request>() {
            @Override
            public Object handleRequest(BizContext bizContext, Request request){
                return handlerRequest(request);
            }
        });
        this.node = node;
    }

    @Override
    public Response<?> handlerRequest(Request request) {
        /**
         *如果是请求投票
         */
        if(request.getCmd()==Request.R_VOTE){
            return new Response<>(node.handlerRequestVote((RvoteParam) request.getObj()));
        }else if(request.getCmd()==Request.A_ENTRIES){//追加日志
            return new Response<>(node.handlerAppendEntries)
        }
    }

    @Override
    public void init(){
        rpcServer.start();
    }

    @Override
    public void destroy(){
        rpcServer.stop();
        log.info("destory success");
    }

}
