package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.entity.NodeConfig;
import com.wangguo.java.raft.server.constant.StateMachineSaveType;
import com.wangguo.java.raft.server.impl.DefaultNode;
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

        /**
         * System.getProperty表示从idea的服务配置中获取参数，默认为8779，如果运行配置中有更改，那么就覆盖
         */
        //自身节点
        config.setSelfPort(Integer.parseInt(System.getProperty("serverPort", "8779")));
        //其他节点地址
        config.setPeerAddrs(Arrays.asList(peerAddr));
        /**
         * 节点初始都是设置为ROCKS_DB状态机类型
         */
        config.setStateMachineSaveType(StateMachineSaveType.ROCKS_DB.getTypeName());

        Node node = DefaultNode.getInstance();
        node.setConfig(config);

        /**
         * 因为是DefaultNode对象（DefaultNode.getInstance();），因此会调用DefaultNode下面的init函数
         * 节点初始化会完成下列步骤：
         * 1、初始化RPC服务
         * 2、初始化一致性模块
         * 3、初始化代理
         * 4、等等
         */
        node.init();//对节点进行初始化
        /**
         * 通过向JVM中添加一个钩子，在JVM关闭之前执行括号里面的线程中的内容。当关闭JVM时候，告诉别人这个节点可以用了
         */
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            synchronized (node){
                node.notifyAll();
            }
        }));
        log.info("gracefully wait");
        /**
         * 一直利用这个节点
         */
        synchronized (node) {
            node.wait();
        }

        /**
         * 优雅地结束
         */
        log.info("gracefully stop");
        node.destroy();
    }
}
