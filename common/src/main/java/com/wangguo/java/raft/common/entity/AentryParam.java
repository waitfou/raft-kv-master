package com.wangguo.java.raft.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 附加(追加）日志的RPC参数，handlerAppendEntries
 */
@Getter
@Setter
@ToString
@Builder
public class AentryParam implements Serializable {
    /**
     * 候选人的任期号
     */
    private long term;
    /**
     * 被请求者ID(ip:selfPort)
     */
    private String serverId;
    /**
     * 领导人的Id,以便于跟随着重定向请求
     */
    private String leaderId;
    /**
     * 新的日志条目跟随之前的索引值
     */
    private long preLogIndex;
    /**
     * prevLogIndex条目的任期号
     */
    private long preLogTerm;
    /**
     * 准备存储的日志条目（表示心跳时为空，一次性发送多个是为了提高效率）
     */
    private LogEntry[] entries;
    /**
     * 领导人已经提交的日志的索引值
     */
    private long leaderCommit;
}
