package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.entity.NodeConfig;
import com.wangguo.java.raft.server.constant.StateMachineSaveType;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;

@Slf4j
public class RaftNodeBootStrap {
    public static void main(String[] args) throws Throwable{

    }
    public static void boot() throws Throwable{
        String property = System.getProperty("cluster.addr.list");
        String[] peerAddr;
        if(StringUtil.isNullOrEmpty(property)){
            peerAddr = new String[]{"localhost:8775", "localhost:8776","localhost:8777","localhost:8778","localhost:8779"};
        }else{
            peerAddr = property.split(",");
        }
        NodeConfig config = new NodeConfig();

        //自身节点
        config.setSelfPort(Integer.parseInt(System.getProperty("serverPort", "8779")));
        //其他节点地址
        config.setPeerAddrs(Arrays.asList(peerAddr));
        config.setStateMachineSaveType(StateMachineSaveType.ROCKS_DB.getTypeName());

        Node node =
    }
}
