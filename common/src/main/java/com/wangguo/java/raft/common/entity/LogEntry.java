package com.wangguo.java.raft.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogEntry implements Serializable, Comparable {
    private Long index;
    private long term;
    private Command command;

    @Override
    public int compareTo(Object o) {
        if(o==null){
            return -1;
        }
        if(this.getIndex()>((LogEntry) o).getIndex()){
            return 1;
        }
        return -1;
    }
}
