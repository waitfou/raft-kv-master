package com.wangguo.java.raft.server.impl;

import com.wangguo.java.raft.server.Consensus;

public class DefaultConsensus implements Consensus {
    public final DefaultNode node;
    public DefaultConsensus(DefaultNode node){
        this.node = node;
    }
}
