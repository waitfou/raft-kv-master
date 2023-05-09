package com.wangguo.java.raft.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@Builder
public class ClientKVReq implements Serializable {
    public static int PUT = 0;
    public static int GET = 1;

    int type;
    String key;
    String value;

    public enum Type{
        PUT(0), GET(1);
        int code;
        Type(int code){
            this.code = code;
        }
        public static Type value(int code){
            for(Type type:values()){
                if(type.code == code){
                    return type;
                }
            }
        }
    }
}
