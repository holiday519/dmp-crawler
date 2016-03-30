package com.pxene.dmp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import cn.wanghaomiao.xpath.model.JXDocument;
import dao.HBaseDAO;
import dao.impl.HBaseDAOImp;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class MyCrawler4hmc extends WebCrawler {
	
	private static HConnection hTablePool = null;
	private static HTableInterface table = null;
	static{
		Configuration conf = null;
		conf = new Configuration();
		String zk_list = "dmp01:2181,dmp02:2181,dmp03:2181,dmp04:2181";
		conf.set("hbase.zookeeper.quorum", zk_list);
		try {
			hTablePool = HConnectionManager.createConnection(conf);
			table = hTablePool.getTable("ee_car");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private final static String REGEX4HMC = "^http://beijing.huimaiche.com/.*?";
	

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().toLowerCase().matches(REGEX4HMC) ;
	}

	@Override
	public void visit(Page page) {
		hmc2hbase(page, REGEX4HMC);
	}

	

	private void hmc2hbase(Page page, String regex) {
		String url = page.getWebURL().getURL();
		// 获取code
		Pattern pattern = Pattern.compile("^http://beijing.huimaiche.com/([\\w|\\d]*)/$");
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {

			System.out.println("#######################" + url);
			String carid = matcher.group(1); // carid
			// System.out.println("carid----"+carid);
			Put put = new Put(("00030007_" + carid).getBytes());

//			put.setWriteToWAL(false); 
			
			// 解析网页
			try {
				Document doc = Jsoup.connect(url).get();

				JXDocument jxDocument = new JXDocument(doc);
				List<Object> names = jxDocument.sel("//div[@class='car-details-info-car-name']/allText()"); // 车名
				if (names.size() > 0) {
					String name = names.get(0).toString();
					put.add("cf1".getBytes(), "name".getBytes(), name.getBytes());
					// System.out.println("name----" + name);
				}

				List<Object> prices = jxDocument.sel("//del[@class='guide-price-value']/text()"); // 购车指导价格
				if (prices.size() > 0) {
					String price = prices.get(0).toString();
					put.add("cf1".getBytes(), "price".getBytes(), price.getBytes());
					// System.out.println("price---" + price);
				}
//				save(put); //入庫
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

	

	public void save(Put put) {
		
		try {
			
			//优化尝试
			table.setWriteBufferSize(6 * 1024 * 1024);  
//			table.setAutoFlush(false);
			HColumnDescriptor hcd = new HColumnDescriptor("cf1");   
			hcd.setCompressionType(Algorithm.SNAPPY);  
			table.put(put);
			table.flushCommits();
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				table.close();
				hTablePool.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
