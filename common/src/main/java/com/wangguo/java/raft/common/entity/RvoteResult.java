package com.wangguo.java.raft.common.entity;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class RvoteResult implements Serializable {
    /**当前任期号*/
    long term;
    /** 候选人是否赢得此张选票 */
    boolean voteGranted;
    public RvoteResult(boolean voteGranted){
        this.voteGranted = voteGranted;
    }
    private RvoteResult(Builder builder){
        setTerm(builder.term);
        setVoteGranted(builder.voteGranted);
    }
    public static RvoteResult fail(){
        return new RvoteResult(false);
    }
    public static RvoteResult ok(){
        return new RvoteResult(true);
    }
    public static Builder newBuilder(){
        return new Builder();
    }
    public static final class Builder{
        private long term;
        private boolean voteGranted;

        private Builder(){

        }
        public Builder term(long term){
            this.term = term;
            return this;
        }
        public Builder voteGranted(boolean voteGranted){
            this.voteGranted = voteGranted;
            return this;
        }
        public RvoteResult build(){
            return new RvoteResult(this);
        }
    }
}
