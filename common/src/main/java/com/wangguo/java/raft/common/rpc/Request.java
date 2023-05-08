package com.wangguo.java.raft.common.rpc;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * 对客户端的请求进行包装
 */
@Builder
@Data
public class Request implements Serializable {
    /**
     * 请求投票
     */
    public static final int R_VOTE = 0;
    /**
     * 附加日志
     */
    public static final int A_ENTRIES =1;
    /**
     * 客户端
     */
    public static final int CLIENT_REQ =2;
    /**
     * 配置变更 add
     */
    public static final int CHANGE_CONFIG_ADD = 3;
    /**
     * 配置变更 remove
     */
    public static final int CHANGE_CONFIG_REMOVE = 4;
    /**
     * 请求类型
     */
    private Object obj;
    private int cmd = -1;
    private String url;
    public Request(int cmd, Object obj, String url){
        this.cmd = cmd;
        this.obj = obj;
        this.url = url;
    }
}
