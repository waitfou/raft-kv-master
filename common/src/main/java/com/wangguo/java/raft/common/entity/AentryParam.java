package com.wangguo.java.raft.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 附加日志的RPC参数，handlerAppendEntries
 */
@Getter
@Setter
@ToString
@Builder
public class AentryParam implements Serializable {
}
