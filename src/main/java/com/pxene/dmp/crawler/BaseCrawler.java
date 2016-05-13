package com.pxene.dmp.crawler;

import java.util.HashMap;
import java.util.Map;

import com.pxene.dmp.common.CrawlerConfig;
import com.pxene.dmp.common.CrawlerConfig.Proxy;

import edu.uci.ics.crawler4j.crawler.WebCrawler;

public class BaseCrawler extends WebCrawler {
	
	protected Proxy proxyConf;
	
	protected BaseCrawler(String confPath) {
		proxyConf = CrawlerConfig.load(confPath).getProxyConf();
	}

	protected Map<String, Map<String, Map<String, byte[]>>> insertData(
			Map<String, Map<String, Map<String, byte[]>>> rowDatas,
			String rowKey, String familyName, String columnName,
			byte[] columnVal) {
		if (rowDatas.containsKey(rowKey)) {
			rowDatas.put(rowKey, insertData(rowDatas.get(rowKey), familyName, columnName, columnVal));
		} else {
			rowDatas.put(rowKey, insertData(new HashMap<String, Map<String, byte[]>>(), familyName, columnName, columnVal));
		}
		return rowDatas;
	}

	protected Map<String, Map<String, byte[]>> insertData(
			Map<String, Map<String, byte[]>> familyDatas, String familyName,
			String columnName, byte[] columnVal) {
		if (familyDatas.containsKey(familyName)) {
			familyDatas.put(familyName, insertData(familyDatas.get(familyName), columnName, columnVal));
		} else {
			familyDatas.put(familyName, insertData(new HashMap<String, byte[]>(), columnName, columnVal));
		}
		return familyDatas;
	}

	protected Map<String, byte[]> insertData(
			Map<String, byte[]> columnDatas, String columnName, byte[] columnVal) {
		columnDatas.put(columnName, columnVal);
		return columnDatas;
	}
	
}
