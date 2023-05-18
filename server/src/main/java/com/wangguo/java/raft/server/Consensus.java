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
     *
     * 接收者实现：
     *  如果term < currentTerm 返回false 不给他投票
     *  如果votedFor为空（也就是我还没约给任何投票）或者自己就是candidateId， 并且候选人的日志至少和自己一样新，那么就投票给他（5.2节， 5.4节）
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
