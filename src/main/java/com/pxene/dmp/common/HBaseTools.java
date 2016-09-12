package com.pxene.dmp.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class HBaseTools {

	private static Configuration conf = new Configuration();
	private static Connection conn = null;
	private static ExecutorService pool = Executors.newFixedThreadPool(200);
	
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
			if (rowKey != null)
			{
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
		}
		try {
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据rowkey查找具体值
	 * @param table	hbase表
	 * @param rowkey 具体的rowkey
	 * @param family 指定列簇
	 * @param row 指定行
	 * @return 返回具体的数据
	 */
	public static Map<String,String> getRowDatas(Table table, String rowkey, byte[] family,
			byte[] row) {
		Map<String,String> datas = new HashMap<String,String>();
		Get get = new Get(row);
		get.addFamily(family);
		// 也可以通过addFamily或addColumn来限定查询的数据
		Result result;
		try {
			result = table.get(get);
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				String key = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				datas.put(key, value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return datas;
	}
	
}
