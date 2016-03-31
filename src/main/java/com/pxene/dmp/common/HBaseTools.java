package com.pxene.dmp.common;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

public class HBaseTools {

	private static Configuration conf = new Configuration();
	private static HConnection conn = null;
	
	static {
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "dmp01,dmp02,dmp03,dmp04,dmp05");
		try {
			conn = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static HTableInterface openTable(String tableName) {
		HTableInterface table = null;
		try {
			table = conn.getTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		table.setAutoFlushTo(false);
		return table;
	}
	
	public static void closeTable(HTableInterface table) {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void putData(HTableInterface table, String key, Map<String, Map<String, byte[]>> data) {
		Put put = new Put(key.getBytes());
		for (Map.Entry<String, Map<String, byte[]>> family : data.entrySet()) {
			String name = family.getKey();
			for (Map.Entry<String, byte[]> column : family.getValue().entrySet()) {
				put.add(name.getBytes(), column.getKey().getBytes(), column.getValue()); 
			}
		}
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
