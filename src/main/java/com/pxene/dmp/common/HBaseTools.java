package com.pxene.dmp.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class HBaseTools {

	private static Configuration conf = new Configuration();
	private static Connection conn = null;
	private static ExecutorService pool = Executors.newFixedThreadPool(160);
	
	static {
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "dmp01,dmp02,dmp03,dmp04,dmp05");
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Table openTable(String tableName) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName), pool);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return table;
	}
	
	public static void closeTable(Table table) {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void putColumnDatas(Table table, String rowKey, String familyName, Map<String, byte[]> columnDatas) {
		Put put = new Put(rowKey.getBytes());
		for (Map.Entry<String, byte[]> columnData : columnDatas.entrySet()) {
			put.addColumn(familyName.getBytes(), columnData.getKey().getBytes(), columnData.getValue());
		}
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void putFamilyDatas(Table table, String rowKey, Map<String, Map<String, byte[]>> familyDatas) {
		Put put = new Put(rowKey.getBytes());
		for (Map.Entry<String, Map<String, byte[]>> familyData : familyDatas.entrySet()) {
			String familyName = familyData.getKey();
			Map<String, byte[]> columnDatas = familyData.getValue();
			for (Map.Entry<String, byte[]> columnData : columnDatas.entrySet()) {
				put.addColumn(familyName.getBytes(), columnData.getKey().getBytes(), columnData.getValue()); 
			}
		}
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void putRowDatas(Table table, Map<String, Map<String, Map<String, byte[]>>> rowDatas) {
		List<Put> puts = new ArrayList<Put>();
		for (Map.Entry<String, Map<String, Map<String, byte[]>>> rowData : rowDatas.entrySet()) {
			String rowKey = rowData.getKey();
			Map<String, Map<String, byte[]>> familyDatas = rowData.getValue();
			Put put = new Put(rowKey.getBytes());
			for (Map.Entry<String, Map<String, byte[]>> familyData : familyDatas.entrySet()) {
				String familyName = familyData.getKey();
				Map<String, byte[]> columnDatas = familyData.getValue();
				for (Map.Entry<String, byte[]> columnData : columnDatas.entrySet()) {
					put.addColumn(familyName.getBytes(), columnData.getKey().getBytes(), columnData.getValue()); 
				}
			}
			puts.add(put);
		}
		try {
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
