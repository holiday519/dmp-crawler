package com.pxene.dmp.crawler.social.currency;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import redis.clients.jedis.JedisPool;

public class Resource
{
    private static final String ZOOKEEPER_CLIENTPORT = "2181";
    private static final String ZOOKEEPER_QUORUMSTRING = "dmp01,dmp02,dmp03,dmp04,dmp05";
    private static final long HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = 1200000;
    
    static JedisPool jedisPool = null;
    static Configuration hbaseConfig = null;
    static Connection connection = null;
    
    public Resource()
    {
        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENTPORT);
        hbaseConfig.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUMSTRING);
        hbaseConfig.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUMSTRING);
        hbaseConfig.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
        
        try
        {
            connection = ConnectionFactory.createConnection(hbaseConfig);
        }
        catch (IOException e)
        {
            connection = null;
        }
    }
    
    public Connection getHBaseConnection()
    {
        return connection;
    }
}
