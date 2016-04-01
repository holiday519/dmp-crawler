package com.pxene.dmp.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

public class HBaseDAOImp {

	static HConnection hTablePool = null;
	static Configuration conf = null;

	static {
		conf = new Configuration();
		String zk_list = "dmp01:2181,dmp02:2181,dmp03:2181,dmp04:2181";
		conf.set("hbase.zookeeper.quorum", zk_list);
		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static  void save(Put put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(put);
			table.flushCommits();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void createTable(String tableName, String[] columnFamilys) {
		try {
			// admin 对象
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				System.err.println("此表，已存在！");
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

				for (String columnFamily : columnFamilys) {
					tableDesc.addFamily(new HColumnDescriptor(columnFamily));
				}

				admin.createTable(tableDesc);
				System.err.println("建表成功!");

			}
			admin.close();// 关闭释放资源
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void deleteTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);// 禁用表
				admin.deleteTable(tableName);// 删除表
				System.err.println("删除表成功!");
			} else {
				System.err.println("删除的表不存在！");
			}
			admin.close();
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String[] cfs = { "auto_info" };
		String name = "t_auto_autoinfo";
		createTable(name, cfs);
	}

}
