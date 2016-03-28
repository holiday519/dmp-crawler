package com.pxene.dmp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import cn.wanghaomiao.xpath.model.JXDocument;
import dao.HBaseDAO;
import dao.impl.HBaseDAOImp;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class MyCrawler extends WebCrawler {

	private final static String REGEX = "^http://www\\.autohome\\.com\\.cn/([\\d]*)/$";
	private static Put put;
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().toLowerCase().matches(REGEX);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		// System.out.println("URL: " + url);
		// 获取code
		Pattern pattern = Pattern.compile(REGEX);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			// System.out.println(url);

			String autoCode = matcher.group(1); // carid
			put = new Put(("00030005_" + autoCode).getBytes());

			String name = "";
			String price = "";
			String other_attention = "";
			String singleScore = "";
			String koubeirank = "";
			String score = "";

			// 解析网页
			try {
				Document doc = Jsoup.connect(url).get();

				JXDocument jxDocument = new JXDocument(doc);
				List<Object> names = jxDocument.sel("//div[@class='subnav-title-name']/a/allText()"); // 车名
				if (names.size() > 0) {
					name = names.get(0).toString();
					put.add("cf1".getBytes(), "name".getBytes(), name.getBytes());
					// System.out.println(name);
				}

				List<Object> prices = jxDocument.sel("//dl/dt/a/text()"); // 购车指导价格
				if (prices.size() > 0) {
					price = prices.get(0).toString();
					put.add("cf1".getBytes(), "price".getBytes(), price.getBytes());
					// System.out.println(price);
				}

				List<Object> others = jxDocument.sel("//div[@class='uibox-con attention']/allText()"); // 关注该车系的还关注的其他车型
				if (others.size() > 0) {
					other_attention = others.get(0).toString();
					put.add("cf1".getBytes(), "other_attention".getBytes(), other_attention.getBytes());
					String[] strs = other_attention.split(" ");
					StringBuffer sb = new StringBuffer("");
					for (int i = 0; i < strs.length; i++) {
						if (strs[i].contains("万起")) {
							continue;
						}
						sb.append(strs[i]).append(",");
					}
					other_attention = sb.substring(0, sb.length() - 1);
					// System.out.println(other_attention);
				}

				List<Object> scores = jxDocument.sel("//a[@class='font-score']/text()"); // 评分
				if (scores.size() > 0) {
					score = scores.get(0).toString();
					put.add("cf1".getBytes(), "score".getBytes(), score.getBytes());
					// System.out.println(score);
				}

				// List<Object> singleScores =
				// jxDocument.sel("//table[@class='table-rank']/allText()"); //
				// 单项评分
				// if (singleScores.size() > 0) {
				// singleScore = singleScores.get(0).toString();
				// put.add("cf1".getBytes(), "singleScore".getBytes(),
				// singleScore.getBytes());
				// // System.out.println(singleScore);
				// }

				// List<Object> koubeiranks =
				// jxDocument.sel("//div[@class='koubei-con-rival']/allText()");
				// // 口碑排行
				// if (koubeiranks.size() > 0) {
				// koubeirank = koubeiranks.get(0).toString();
				// put.add("cf1".getBytes(), "koubeirank".getBytes(),
				// koubeirank.getBytes());
				// System.out.println(koubeirank);
				// }

				// 入库
				save(put, "ee_car");
				System.out.println("#######################" + url);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public void save(Put put, String tableName) {

		HConnection hTablePool = null;
		Configuration conf = null;

		conf = new Configuration();
		String zk_list = "dmp01:2181,dmp02:2181,dmp03:2181,dmp04:2181";
		conf.set("hbase.zookeeper.quorum", zk_list);
		// conf.set("dmapreduce.job.queuename", "dmp1");
		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(put);
			table.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//关闭资源
		try {
			hTablePool.close();
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
