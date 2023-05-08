package com.wangguo.java.raft.server;

import com.wangguo.java.raft.common.entity.AentryResult;
import com.wangguo.java.raft.common.entity.RvoteParam;
import com.wangguo.java.raft.common.entity.RvoteResult;

public interface Consensus{
    RvoteResult requestVote(RvoteParam param);
    AentryResult appendEntries(AentryParam param);
}
