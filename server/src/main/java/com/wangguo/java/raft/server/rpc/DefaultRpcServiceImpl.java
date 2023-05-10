package com.wangguo.java.raft.server.rpc;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.RpcServer;
import com.wangguo.java.raft.common.entity.AentryParam;
import com.wangguo.java.raft.common.entity.ClientKVReq;
import com.wangguo.java.raft.common.entity.Peer;
import com.wangguo.java.raft.common.entity.RvoteParam;
import com.wangguo.java.raft.common.rpc.Request;
import com.wangguo.java.raft.common.rpc.Response;
import com.wangguo.java.raft.server.changes.ClusterMembershipChanges;
import com.wangguo.java.raft.server.impl.DefaultNode;
import lombok.extern.slf4j.Slf4j;

/**
 * Raft Server
 */
@Slf4j
public class DefaultRpcServiceImpl implements RpcService {
    private final DefaultNode node;
    private final RpcServer rpcServer;

    public DefaultRpcServiceImpl(int port, DefaultNode node) {
        rpcServer = new RpcServer(port, false, false);
        rpcServer.registerUserProcessor(new RaftUserProcessor<Request>() {
            @Override
            public Object handleRequest(BizContext bizContext, Request request) {
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
        if (request.getCmd() == Request.R_VOTE) {
            return new Response<>(node.handlerRequestVote((RvoteParam) request.getObj()));
        } else if (request.getCmd() == Request.A_ENTRIES) {//追加日志
            return new Response<>(node.handlerAppendEntries((AentryParam) request.getObj()));
        } else if (request.getCmd() == Request.CLIENT_REQ) {
            return new Response<>(node.handlerClientRequest((ClientKVReq) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_REMOVE) {
            return new Response<>(((ClusterMembershipChanges) node).removePeer((Peer) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_ADD) {
            return new Response<>(((ClusterMembershipChanges) node).addPeer((Peer) request.getObj())); // 子类可以向父类转型
        }
        return null;
    }

    @Override
    public void init() {
        rpcServer.start();
    }

    @Override
    public void destroy() {
        rpcServer.stop();
        log.info("destory success");
    }

}
