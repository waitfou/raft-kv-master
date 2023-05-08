package com.wangguo.java.raft.server.exception;

public class RaftNotSupportException extends RuntimeException{
    public RaftNotSupportException(){

    }
    public RaftNotSupportException(String message){
        super(message);
    }
}
