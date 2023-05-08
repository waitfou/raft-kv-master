package com.wangguo.java.raft.common.entity;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@Builder
@Data
public class RvoteParam implements Serializable {
    /**
     * 候选人的任期号
     */
    private long term;
    /**
     * 被请求者ID(ip:selfPort)
     */
    private String serverId;
    /**
     * 请求选票的候选人的Id(ip:selfPort)
     */
    private String candidateId;
    /**
     * 候选人的最后日志条目的索引值
     */
    private long lastLogIndex;
    /**
     * 候选者最后日志条目的任期号
     */
    private long lastLogTerm;
}
