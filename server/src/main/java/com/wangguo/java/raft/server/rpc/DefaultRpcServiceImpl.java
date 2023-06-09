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
 * Raft Server 这个类主要用于处理各种不同请求的不同处理方法
 */
@Slf4j
public class DefaultRpcServiceImpl implements RpcService {
    private final DefaultNode node;
    // 蚂蚁金服RPC框架的服务器处理端
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

    // 接收来自客户端的请求，并进行处理
    @Override
    public Response<?> handlerRequest(Request request) {

        if (request.getCmd() == Request.R_VOTE) { // 处理投票
            return new Response<>(node.handlerRequestVote((RvoteParam) request.getObj()));
        } else if (request.getCmd() == Request.A_ENTRIES) {//追加日志
            return new Response<>(node.handlerAppendEntries((AentryParam) request.getObj()));
        } else if (request.getCmd() == Request.CLIENT_REQ) { // 处理客户端请求
            return new Response<>(node.handlerClientRequest((ClientKVReq) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_REMOVE) { // 处理改变配置
            return new Response<>(((ClusterMembershipChanges) node).removePeer((Peer) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_ADD) { // 集群中添加了节点
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
