package com.pxene.dmp.crawler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.pxene.dmp.common.CrawlerConfig;
import com.pxene.dmp.common.CrawlerConfig.LoginConf;
import com.pxene.dmp.common.CrawlerConfig.ProxyConf;

import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class BaseCrawler extends WebCrawler {
	
	protected LoginConf loginConf;
	protected ProxyConf proxyConf;
	
	protected BaseCrawler(String confPath) {
		CrawlerConfig conf = CrawlerConfig.load(confPath);
		loginConf = conf.getLoginConf();
		proxyConf = conf.getProxyConf();
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
	
	protected Document parseHtml(String html) {
		return Jsoup.parse(html);
	}
	
	protected Document connectUrl(String url) {
		Document doc = null;
		try {
			doc = Jsoup.connect(url).userAgent("chrome").timeout(20000).get();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return doc;
	}
	
	protected Document connectUrl(String url, Map<String, String> cookie) {
		Document doc = null;
		try {
			doc = Jsoup.connect(url).cookies(cookie).timeout(20000).get();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return doc;
	}
	
	@Override
	protected void onContentFetchError(WebURL webUrl) {
		if (proxyConf.isEnable()) {
			String[] params = proxyConf.randomIp().split(":");
			System.getProperties().setProperty("proxySet", "true");
			System.getProperties().setProperty("http.proxyHost", params[0]);
			System.getProperties().setProperty("http.proxyPort", params[1]);
		}
	}
}
