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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;


public class HBaseTools {

	public static Configuration conf = new Configuration();
	private static Connection conn = null;
	private static ExecutorService pool = Executors.newFixedThreadPool(200);
	
	static {
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "dmp01,dmp02,dmp03,dmp04,dmp05");
		conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000);
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
	
	/**
     * 向HBase中插入构造好的数据。
     * @param tblName       HBase的表名
     * @param preparedData  准备好的数据
     * @throws IOException
     */
    public static void insertIntoHBase(String tblName, Map<String, Map<String, Map<String, byte[]>>> preparedData) throws IOException
    {
        if (preparedData.size() > 0) 
        {
            Table table = openTable(tblName);
            if (table != null) 
            {
                putRowDatas(table, preparedData);
                closeTable(table);
            }
        }
    }
    
    
    public static Map<String, Map<String, Map<String, byte[]>>> insertData(Map<String, Map<String, Map<String, byte[]>>> rowDatas, String rowKey, String familyName, String columnName, byte[] columnVal)
    {
        if (rowDatas.containsKey(rowKey))
        {
            rowDatas.put(rowKey, insertData(rowDatas.get(rowKey), familyName, columnName, columnVal));
        }
        else
        {
            rowDatas.put(rowKey, insertData(new HashMap<String, Map<String, byte[]>>(), familyName, columnName, columnVal));
        }
        return rowDatas;
    }
    
    
    public static Map<String, Map<String, byte[]>> insertData(Map<String, Map<String, byte[]>> familyDatas, String familyName, String columnName, byte[] columnVal)
    {
        if (familyDatas.containsKey(familyName))
        {
            familyDatas.put(familyName, insertData(familyDatas.get(familyName), columnName, columnVal));
        }
        else
        {
            familyDatas.put(familyName, insertData(new HashMap<String, byte[]>(), columnName, columnVal));
        }
        return familyDatas;
    }
    
    
    public static Map<String, byte[]> insertData(Map<String, byte[]> columnDatas, String columnName, byte[] columnVal)
    {
        columnDatas.put(columnName, columnVal);
        return columnDatas;
    }
	
}
