package com.wangguo.java.raft.common.rpc;

import com.wangguo.java.raft.server.changes.Result;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
@Getter
@Setter
public class Response<T> implements Serializable { //回复的类型通过T定制化
    private T result;
    public Response(T result){
        this.result = result;
    }
    private Response(Builder builder){
        setResult((T)builder.result);
    }
    public static Response<String> ok(){
        return new Response<>("ok");
    }
    public static Response<String> fail(){return new Response<>("fail");}

    public static Builder newBuilder(){
        return new Builder();
    }
    public Response() {
        super();
    }
    public static final class Builder{
        private Object result;
        private Builder(){

        }
        public Builder result(Object val){
            result = val;
            return this;
        }
        public Response<?> build(){
            return new Response(this);
        }
    }
    @Override
    public String toString() {
        return "Response{" +
                "result=" + result +
                '}';
    }

}
