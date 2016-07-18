package com.pxene.dmp.crawler.social.currency;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Resource
{
    private static final String REDIS_HOST = "192.168.3.176";
    private static final int REDIS_PORT = 6379;
    
    JedisPool jedisPool = null;
    
    public Resource()
    {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(50000);
        config.setMaxIdle(10);
        config.setMaxWaitMillis(10 * 1000);
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, REDIS_HOST, REDIS_PORT);
    }
    
    public JedisPool getJedisPool()
    {
        return this.jedisPool;
    }
}
