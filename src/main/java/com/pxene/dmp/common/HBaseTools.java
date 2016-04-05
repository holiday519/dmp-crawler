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
	
	public static void addColumnDatas(HTableInterface table, String rowKey, String familyName, Map<String, byte[]> columnDatas) {
		Put put = new Put(rowKey.getBytes());
		for (Map.Entry<String, byte[]> data : columnDatas.entrySet()) {
			put.add(familyName.getBytes(), data.getKey().getBytes(), data.getValue()); 
		}
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
