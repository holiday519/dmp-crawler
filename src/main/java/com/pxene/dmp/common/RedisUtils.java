package com.pxene.dmp.common;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis操作类
 * 
 * @author zhuzi
 *
 */
public class RedisUtils {
	private static JedisPool pool = null;

	/**
	 * 获取jedis连接池
	 * */
	public static JedisPool getPool() {
		if (pool == null) {
			// 创建jedis连接池配置
			JedisPoolConfig config = new JedisPoolConfig();
			// 最大连接数
			config.setMaxTotal(100);
			// 最大空闲连接
			config.setMaxIdle(5);
			// 创建redis连接池
			pool = new JedisPool(config, "192.168.3.176", 6379, 20000);
		}
		return pool;
	}

	/**
	 * 获取jedis连接
	 * */
	public static Jedis getConn() {
		return getPool().getResource();
	}
	
	
}
