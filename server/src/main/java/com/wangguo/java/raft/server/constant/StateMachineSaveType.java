package com.wangguo.java.raft.server.constant;

import com.wangguo.java.raft.server.StateMachine;
import com.wangguo.java.raft.server.impl.DefaultStateMachine;
import com.wangguo.java.raft.server.impl.RedisStateMachine;
import lombok.Getter;

/**
 * 快照存储类型
 */
@Getter
public enum StateMachineSaveType {
    REDIS("redis","redis存储",RedisStateMachine.getInstance()),

    ROCKS_DB("RocksDB", "RocksDB本地存储", DefaultStateMachine.getInstance());

    public StateMachine getStateMachine() {
        return this.stateMachine;
    }

    private String typeName;
    private String desc;
    private StateMachine stateMachine;

    public StateMachineSaveType(String typeName, String desc, StateMachine stateMachine) {
        this.typeName = typeName;
        this.desc = desc;
        this.stateMachine = stateMachine;
    }

    public static StateMachineSaveType getForType(String typeName) {
        for (StateMachineSaveType value : values()) {
            if (value.getTypeName().equals(typeName)) {
                return value;
            }
        }
        return null;
    }
}
