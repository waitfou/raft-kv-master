package com.wangguo.java.raft.common.entity;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 节点集合，去重，设置自我节点，管理集群中的其他节点，更换Leader节点
 */
public class PeerSet implements Serializable {
    private List<Peer> list = new ArrayList<>();
    private volatile Peer leader;
    /**
     * 自我节点。根据具体场景设置的不同可动态变化
     */
    private volatile Peer self;
    private PeerSet(){

    }
    public static PeerSet getInstance(){
        return PeerSetLazyHolder.INSTANCE;
    }
    //懒汉式单例模式创建
    private static class PeerSetLazyHolder{
        private static final PeerSet INSTANCE = new PeerSet();
    }
    public void setSelf(Peer peer){
        self = peer;
    }

    /**
     * 移除节点
     * @param peer
     */
    public void removePeer(Peer peer){
        list.remove(peer);
    }

    /**
     * 获取节点列表
     * @return
     */
    public List<Peer> getPeers(){
        return list;
    }
    /**
     * 获取除了自我节点之外的其他节点
     */
    public List<Peer> getPeersWithOutSelf(){
        List<Peer> list2 = new ArrayList<>(list);
        list2.remove(self);
        return list2;
    }

    /**
     * 获取当前集群的领导者
     * @return
     */
    public Peer getLeader(){
        return leader;
    }

    /**
     * 设置当前集群的Leader
     * @param peer
     */
    public void setLeader(Peer peer){
        leader = peer;
    }

    @Override
    public String toString() {
        return "PeerSet{" +
                "list=" + list +
                ", leader=" + leader +
                ", self=" + self +
                '}';
    }
}
