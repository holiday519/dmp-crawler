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
	//0号数据库的变量
	public static final String ARTICLE_ID_LIST = "article_id_list";

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
			pool = new JedisPool(config, "115.182.33.163", 7000, 20000);
		}
		return pool;
	}

	/**
	 * 获取jedis连接
	 * */
	public static Jedis getConn() {
		return getPool().getResource();
	}
	
	/**
	 * 向指定的list变量里添加值
	 * @param key	指定变量
	 * @param value	具体值
	 */
	public static void jedisPushToList(String key, String value, int dbindex){
		Jedis conn = getConn();
		conn.select(dbindex);
		conn.lpush(key, value);
		conn.close();
	}
	
	
}
