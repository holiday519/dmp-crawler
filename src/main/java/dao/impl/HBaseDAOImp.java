package dao.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import dao.HBaseDAO;

public class HBaseDAOImp implements HBaseDAO {

	HConnection hTablePool = null;
	static Configuration conf = null;
	

	public HBaseDAOImp() {
		conf = new Configuration();
		String zk_list = "dmp01:2181,dmp02:2181,dmp03:2181,dmp04:2181";
		conf.set("hbase.zookeeper.quorum", zk_list);
//		conf.set("dmapreduce.job.queuename", "dmp1");
		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public void save(Put put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
			try {
				table = hTablePool.getTable(tableName);
				table.put(put);
				table.flushCommits();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	
	public void insert(String tableName, String rowKey, String family, String quailifer, String value) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes());
			table.put(put);
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

	
	public void insert(String tableName, String rowKey, String family, String quailifer[], String value[]) {
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			// 批量添加
			for (int i = 0; i < quailifer.length; i++) {
				String col = quailifer[i];
				String val = value[i];
				put.add(family.getBytes(), col.getBytes(), val.getBytes());
			}
			table.put(put);
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

	public void save(List<Put> Put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(Put);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	
	public Result getOneRow(String tableName, String rowKey) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			rsResult = table.get(get);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}

	
	public Result getOneRowAndMultiColumn(String tableName, String rowKey, String cf, String[] cols) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			for (int i = 0; i < cols.length; i++) {
				get.addColumn(cf.getBytes(), cols[i].getBytes());
			}
			rsResult = table.get(get);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}

	
	public List<Result> getRows(String tableName, String rowKeyLike) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rs : scanner) {
				list.add(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	
	public List<Result> getRows(String tableName, String rowKeyLike, String cols[]) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes());
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rs : scanner) {
				list.add(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	
	public List<Result> getRowsByOneKey(String tableName, String rowKeyLike, String cols[]) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes());
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rs : scanner) {
				list.add(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	
	public List<Result> getRows(String tableName, String startRow, String stopRow) {
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			Scan scan = new Scan();
			scan.setStartRow(startRow.getBytes());
			scan.setStopRow(stopRow.getBytes());
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rsResult : scanner) {
				list.add(rsResult);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	
	public void deleteRecords(String tableName, String rowKeyLike) {
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			List<Delete> list = new ArrayList<Delete>();
			for (Result rs : scanner) {
				Delete del = new Delete(rs.getRow());
				list.add(del);
			}
			table.delete(list);
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

	public void createTable(String tableName, String[] columnFamilys) {
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

	/**
	 * 删除一个表
	 * 
	 * @param tableName
	 *            删除的表名
	 */
	public void deleteTable(String tableName) {
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

	/**
	 * 查询表中所有行
	 * 
	 * @param tablename
	 */
	public void scaner(String tablename) {
		try {
			HTable table = new HTable(conf, tablename);
			Scan scan = new Scan();

//			// 前置过滤器：针对rowkey
//			PrefixFilter pFilter = new PrefixFilter(Bytes.toBytes("20121203"));
//			scan.setFilter(pFilter);
//
//			// 确定值 行过滤器 针对rowkey
//			RowFilter rFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("2012120312")));
//			scan.setFilter(rFilter);
//
//			// 单值过滤器 1 完整匹配字节数组—针对rowkey
//			RowFilter rsubFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("01212"));
//			scan.setFilter(rsubFilter);
//
//			// 单值过滤器2 匹配正则表达式---针对rowkey
//			RowFilter rGexFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*23"));
//			scan.setFilter(rGexFilter);
//
//			// family过滤----前缀过滤数据
//			FamilyFilter familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("base")));
//			scan.setFilter(familyFilter);
//
//			// 根据qualifier值过滤 完整匹配字节数组—--取出的是一列的值
//			SingleColumnValueFilter sFilter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("cjh312")));
//			scan.setFilter(sFilter);
//
//			// 根据qualifier值过滤 匹配正则表达式—--取出的是一列的值
//			SingleColumnValueFilter singleFilter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"), CompareOp.EQUAL, new RegexStringComparator(".*23"));
//			scan.setFilter(singleFilter);
//
//			// 根据qualifier名过滤---前缀过滤数据
//			QualifierFilter qualifierFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("age")));
//			scan.setFilter(qualifierFilter);

			// s.addColumn(family, qualifier)
			// s.addColumn(family, qualifier)
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {

				for (Cell cell : r.rawCells()) {
					System.out.println("RowKey:" + new String(CellUtil.cloneRow(cell)) + " ");
					// System.out.println("Timetamp:" + cell.getTimestamp() + "
					// ");
					// System.out.println("column Family:" + new
					// String(CellUtil.cloneFamily(cell)) + " ");
					// System.out.println("row Name:" + new
					// String(CellUtil.cloneQualifier(cell)) + " ");
					System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void scanerByColumn(String tablename, String cf, String... columns) {

		try {
			HTable table = new HTable(conf, tablename);
			Scan s = new Scan();
			for (int i = 0; i < columns.length; i++) {
				s.addColumn(cf.getBytes(), columns[i].getBytes());
			}

			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {

				for (Cell cell : r.rawCells()) {
					System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
					System.out.println("Timetamp:" + cell.getTimestamp() + " ");
					System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
					System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
					System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

}
