package com.pxene.dmp.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

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
	
	public static void putColumnDatas(HTableInterface table, String rowKey, String familyName, Map<String, byte[]> columnDatas) {
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
	
//	public static String[] getRowKeys(HTableInterface table, String prefix) {
//		List<String> rowKeys = new ArrayList<String>();
//		Scan scan = new Scan();
//		scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
//		try {
//			ResultScanner scanner = table.getScanner(scan);
//			for (Result result : scanner) {
//				rowKeys.add(new String(result.getRow()));
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		return (String[])rowKeys.toArray();
//	}
//	
//	public static void putRowDatas(HTableInterface table, Map<String, Map<String, Map<String, byte[]>>> rowDatas) {
//		List<Put> puts = new ArrayList<Put>();
//		for (Map.Entry<String, Map<String, Map<String, byte[]>>> rowData : rowDatas.entrySet()) {
//			String rowKey = rowData.getKey();
//		}
//	}
}
