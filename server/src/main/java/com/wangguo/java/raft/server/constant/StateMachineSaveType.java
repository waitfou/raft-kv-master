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
    /** 枚举类中的成员 */
    REDIS("redis","redis存储",RedisStateMachine.getInstance()), //redis状态机
    ROCKS_DB("RocksDB", "RocksDB本地存储", DefaultStateMachine.getInstance()); //默认状态机

    public StateMachine getStateMachine() {
        return this.stateMachine;
    }

    private String typeName;
    private String desc;
    private StateMachine stateMachine;

    StateMachineSaveType(String typeName, String desc, StateMachine stateMachine) {
        this.typeName = typeName;
        this.desc = desc;
        this.stateMachine = stateMachine;
    }

    public static StateMachineSaveType getForType(String typeName) {
        //遍历每种状态机类型
        for (StateMachineSaveType value : values()) {
            if (value.getTypeName().equals(typeName)) {
                return value; //匹配上就返回状态机信息
            }
        }
        return null;
    }
}
