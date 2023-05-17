package com.wangguo.java.raft.client;

import com.google.common.collect.Lists;
import com.wangguo.java.raft.common.entity.ClientKVAck;
import com.wangguo.java.raft.common.entity.ClientKVReq;
import com.wangguo.java.raft.common.entity.LogEntry;
import com.wangguo.java.raft.common.rpc.DefaultRpcClient;
import com.wangguo.java.raft.common.rpc.Request;
import com.wangguo.java.raft.common.rpc.RpcClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RaftClientRPC {
    private static List<String> list = Lists.newArrayList("localhost:8777", "localhost:8778", "localhost:8779");

    private final static RpcClient CLIENT = new DefaultRpcClient();
    private AtomicLong count = new AtomicLong(3);

    public RaftClientRPC() throws Throwable {
        CLIENT.init();
    }

    /**
     * 从服务器集群中获取数据
     * @param key
     * @return
     */
    public LogEntry get(String key) {
        ClientKVReq obj = ClientKVReq.builder().key(key).type(ClientKVReq.GET).build();
        int index = (int) (count.incrementAndGet() % list.size());

        String addr = list.get(index);
        ClientKVAck response;
        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        try{
            response = CLIENT.send(r);
        } catch (Exception e){
            r.setUrl(list.get(((int)(count.incrementAndGet()) % list.size())));
            response = CLIENT.send(r);
        }
        return (LogEntry) response.getResult();
    }

    /**
     * 向服务器集群节点存放数据
     * @param key
     * @param value
     * @return
     */
    public String put(String key, String value) {
        // 采用的是轮询,如果节点不是Leader节点，那么存放的消息会被转发到Leader节点。
        int index = (int) (count.incrementAndGet() % list.size());
        String addr = list.get(index);
        ClientKVReq obj = ClientKVReq.builder().key(key).value(value).type(ClientKVReq.PUT).build();

        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        ClientKVAck response;
        try{
            response = CLIENT.send(r); //这里发送之后。服务端的handlerRequest(Request request)就会处理，具体见蚂蚁金服sofa-bolt官方文档
        }catch (Exception e) {
            r.setUrl(list.get((int)((count.incrementAndGet())%list.size())));
            response = CLIENT.send(r);
        }
        return response.getResult().toString();
    }
}
