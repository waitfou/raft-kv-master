package com.wangguo.java.raft.common.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString
public class NodeConfig {
    /**自身端口*/
    public int selfPort;

    /**所有节点地址*/
    public List<String> peerAddrs;

    /**状态快照存储类型*/
    public String stateMachineSaveType;
}
