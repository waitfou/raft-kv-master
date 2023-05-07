package com.wangguo.java.raft.server.impl;

import com.alibaba.fastjson.JSON;
import com.wangguo.java.raft.common.entity.Command;
import com.wangguo.java.raft.common.entity.LogEntry;
import com.wangguo.java.raft.server.StateMachine;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * redis实现状态机存储
 */
@Slf4j
public class RedisStateMachine implements StateMachine {
    private JedisPool jedisPool;

    private RedisStateMachine() {
        init();
    }

    public static RedisStateMachine getInstance() {
        return RedisStateMachineLazyHolder.INSTANCE;
    }

    private static class RedisStateMachineLazyHolder {
        private static final RedisStateMachine INSTANCE = new RedisStateMachine();
    }

    @Override
    public void init() {
        //配置redis
        GenericObjectPoolConfig redisConfig = new GenericObjectPoolConfig();
        redisConfig.setMaxTotal(100);
        redisConfig.setMaxWaitMillis(10 * 1000);
        redisConfig.setMaxIdle(100);
        redisConfig.setTestOnBorrow(true);
        // todo config
        jedisPool = new JedisPool(redisConfig, System.getProperty("redis.host", "127.0.0.1"), 6379)
    }

    @Override
    public void destroy() throws Throwable {
        //生命周期结束、关闭jedisPool
        jedisPool.close();
        log.info("destory success");
    }

    @Override
    public void apply(LogEntry logEntry) {
        //获取资源
        try (Jedis jedis = jedisPool.getResource()) {
            Command command = logEntry.getCommand();
            if (command == null) {
                log.warn("insert no-op log, logEntry={}", logEntry);
                return;
            }
            String key = command.getKey();
            //把日志实体转换成JSON对象保存到redis中,
            jedis.set(key.getBytes(), JSON.toJSONBytes(logEntry));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过键获取日志
     */
    @Override
    public LogEntry get(String key) {
        LogEntry result = null;
        try (Jedis jedis = jedisPool.getResource()) {
            //将JSON的数据还原成Java对象
            result = JSON.parseObject(jedis.get(key), LogEntry.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public String getString(String key) {
        String result = null;
        try (Jedis jedis = jedisPool.getResource()) {
            result = jedis.get(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void setString(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 在redis中删除键值对
     */
    @Override
    public void delString(String... keys) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
