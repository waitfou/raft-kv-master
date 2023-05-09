package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.entity.AentryParam;
import com.wangguo.java.raft.common.entity.AentryResult;
import com.wangguo.java.raft.common.entity.RvoteParam;
import com.wangguo.java.raft.common.entity.RvoteResult;

/**
 * Raft一致性模块
 */
public interface Consensus{
    /**
     * 请求投票RPC
     * @param param
     * @return
     */
    RvoteResult requestVote(RvoteParam param);

    /**
     * 附加日志（多个日志，为了提高效率）RPC
     * @param param
     * @return
     */
    AentryResult appendEntries(AentryParam param);
}
