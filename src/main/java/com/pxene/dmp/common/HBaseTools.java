package com.pxene.dmp.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.pxene.dmp.crawler.ms.domain.Article;
import com.pxene.dmp.domain.Weixin;

public class HBaseTools {

	private static Configuration conf = new Configuration();
	private static Connection conn = null;
	private static ExecutorService pool = Executors.newFixedThreadPool(200);
	
	static {
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "dmp01,dmp02,dmp03,dmp04,dmp05");
		conf.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,120000);
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
	
	
	public static void filterSingleColumnValueFilter(String tableName){
	    HTable hTable = null;
	    ResultScanner rs = null;
	    try {
	        hTable = new HTable(conf, tableName);
	        FilterList filterList = new FilterList();
	        Filter filter1 = new SingleColumnValueFilter("info".getBytes(), "article_date".getBytes(), CompareOp.GREATER_OR_EQUAL, "2016-07-01".getBytes());
	        filterList.addFilter(filter1);
	        Filter filter2 = new SingleColumnValueFilter("info".getBytes(), "article_date".getBytes(), CompareOp.LESS_OR_EQUAL, "2016-07-31".getBytes());
	        filterList.addFilter(filter2);
	        Scan scan = new Scan();
	        scan.setFilter(filterList);
	        rs = hTable.getScanner(scan);
	        KeyWordComputer kwc = new KeyWordComputer(20);
	        for (Result result : rs) {
	        	Article article = new Article();
	            for (KeyValue kv : result.raw()) {
	                String rowkey = Bytes.toString(kv.getRow());
	                if(article.getId() == null){
	                	article.setId(rowkey);
	                }
	                String qualifier = Bytes.toString(kv.getQualifier());
	                String value = Bytes.toString(kv.getValue());
	                if(qualifier.equals("article_title")){
	                	article.setTitle(value);
	                }else if(qualifier.equals("article_content")){
	                	article.setContent(value);
	                }else if(qualifier.equals("article_date")){
	                	article.setTime(value);
	                }
	            }
	            Collection<Keyword> collection_kw = kwc.computeArticleTfidf(article.getTitle(), article.getContent());
	            article.setContent("");
	            Iterator<Keyword> kv_iter = collection_kw.iterator();
	            while(kv_iter.hasNext()){
	            	Keyword kv = kv_iter.next();
	            	article.setContent(article.getContent() + kv.getName());
	            }
	            System.out.println("id:"+article.getId()+",content:"+article.getContent()+",time:"+article.getTime());
	            System.err.println("=================================================================");
	            SolrUtil.addIndex(article);
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }finally{
	        rs.close();
	        try {
	            hTable.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	}
	
	//扫描hbase表，获取所有记录
	public static void getAllRow(String tableName){
	    HTable hTable = null;
	    ResultScanner rs = null;
	    try {
	        hTable = new HTable(conf, tableName);
	        rs = hTable.getScanner(new Scan());
	        //循环rowkey
	        for (Result result : rs) {
	        	Weixin weixin = new Weixin();
	            for (KeyValue kv : result.raw()) {
	            	String rowkey = Bytes.toString(kv.getRow());
	            	if(weixin.getId() == null){
	            		weixin.setId(rowkey);
	            	}
	            	String qualifier = Bytes.toString(kv.getQualifier());
	                String value = Bytes.toString(kv.getValue());
	                if(qualifier.equals("biz")){
	                	weixin.setBiz(value);
	                }else if(qualifier.equals("nickname")){
	                	weixin.setNickname(value);
	                }
	            }
	            System.out.println("id:"+weixin.getId()+",biz:"+weixin.getBiz()+",nickname:"+weixin.getNickname());
	            System.err.println("=================================================================");
	            SolrUtil.addIndex(weixin);
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally{
	        rs.close();
	        try {
	            hTable.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	}

	
	public static void main(String[] args) {
		filterSingleColumnValueFilter("t_prod_weixin_art");
//		getAllRow("t_prod_weixin_biz");
	}
}
