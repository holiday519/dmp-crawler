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

public class MyCrawler4auto extends WebCrawler {
	
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
	private final static String REGEX4AUTO = "^http://www.autohome.com.cn/car/$";
	
	

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().toLowerCase().matches(REGEX4AUTO) ;
	}

	@Override
	public void visit(Page page) {
//		autohome2hbase(page, REGEX4AUTO);
		String url = page.getWebURL().getURL();
		Document doc;
		try {
			doc = Jsoup.connect(url).get();
			JXDocument jxDocument = new JXDocument(doc);
			
			List<Object> infos = jxDocument.sel("//li/h4/a/allText()"); // 信息
			for (Object info : infos) {
				// System.out.println(price);
				System.out.println(info);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	

	private void autohome2hbase(Page page, String regex) {
		String url = page.getWebURL().getURL();
		// System.out.println("URL: " + url);
		// 获取code
		Pattern pattern = Pattern.compile("^http://www\\.autohome\\.com\\.cn/([\\d]*)/$");
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			String autoCode = matcher.group(1); // carid
			System.out.println("#######################" + url);
			Put put = new Put(("00030005_" + autoCode).getBytes());
			
//			put.setWriteToWAL(false); 
			// 解析网页
			try {
				Document doc = Jsoup.connect(url).get();

				JXDocument jxDocument = new JXDocument(doc);
				List<Object> names = jxDocument.sel("//div[@class='subnav-title-name']/a/allText()"); // 车名
				if (names.size() > 0) {
					String name = names.get(0).toString();
					put.add("cf1".getBytes(), "name".getBytes(), name.getBytes());
					// System.out.println(name);
				}

				List<Object> prices = jxDocument.sel("//dl/dt/a/text()"); // 购车指导价格
				if (prices.size() > 0) {
					String price = prices.get(0).toString();
					put.add("cf1".getBytes(), "price".getBytes(), price.getBytes());
					// System.out.println(price);
				}

				List<Object> others = jxDocument.sel("//div[@class='uibox-con attention']/allText()"); // 关注该车系的还关注的其他车型
				if (others.size() > 0) {
					String other_attention = others.get(0).toString();
					String[] strs = other_attention.split(" ");
					StringBuffer sb = new StringBuffer("");
					for (int i = 0; i < strs.length; i++) {
						if (strs[i].contains("万起")) {
							continue;
						}
						sb.append(strs[i]).append(",");
					}
					other_attention = sb.substring(0, sb.length() - 1);
					put.add("cf1".getBytes(), "other_attention".getBytes(), other_attention.getBytes());
					// System.out.println(other_attention);
				}

				List<Object> scores = jxDocument.sel("//a[@class='font-score']/text()"); // 评分
				if (scores.size() > 0) {
					String score = scores.get(0).toString();
					put.add("cf1".getBytes(), "score".getBytes(), score.getBytes());
					// System.out.println(score);
				}

//				 save(put); //入庫

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
