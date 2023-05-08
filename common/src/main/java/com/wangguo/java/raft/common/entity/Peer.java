package com.wangguo.java.raft.common.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * 当前节点的同伴（也就是其他节点）
 */
@Getter
@Setter
public class Peer {
    private final String addr; //节点成员的地址

    public Peer(String addr) {
        this.addr = addr;
    }

    @Override
    public boolean equals(Object o) { //Object类是任何类的父类，因此可以用Object指代任何类
        if (this == o) {
            return true;
        }
        if(o==null||getClass()!=o.getClass()){ //getClass用于获取对象的运行时对象的类
            return false;
        }
        Peer peer = (Peer) o;
        return Objects.equals(addr, peer.addr); //用于比较两个对象的值是否相等
    }
    @Override
    public int hashCode(){
        return Objects.hash(addr);
    }

    @Override
    public String toString() {
        return "Peer{" +
                "addr='" + addr + '\'' +
                '}';
    }
}
