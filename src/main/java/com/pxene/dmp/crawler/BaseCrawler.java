package com.pxene.dmp.crawler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.pxene.dmp.common.CookieTools;
import com.pxene.dmp.common.CrawlerConfig;
import com.pxene.dmp.common.CrawlerConfig.Login;
import com.pxene.dmp.common.CrawlerConfig.Proxy;

import edu.uci.ics.crawler4j.crawler.WebCrawler;

public class BaseCrawler extends WebCrawler {
	// 抓取失败重试最大次数
	private static final int RETRY_MAXIMUM = 20;

	private static final int TIME_WAIT = 500;
	private static final int TIME_OUT = 3000;
	
	private static final String ROOT_PATH = "/com/pxene/dmp/crawler/";

	protected Document getDocument(String url, String configPath, String cookiePath) {
		CrawlerConfig config = CrawlerConfig.load(ROOT_PATH + configPath);
		Connection conn = Jsoup.connect(url).timeout(TIME_OUT);
		for (int i = 0; i < RETRY_MAXIMUM; i++) {
			try {
				return conn.get();
			} catch (IOException e) {
				// 请求失败，重试
				// 判断是否需要登录
				Login loginConf = config.getLoginConf();
				if (loginConf.isEnable()) {
					conn.cookies(CookieTools.getCookie(ROOT_PATH + cookiePath));
				}
				// 判断是否需要代理
				Proxy proxyConf = config.getProxyConf();
				if (proxyConf.isEnable()) {
					conn.userAgent(proxyConf.randomUserAgent());
					String[] args = proxyConf.randomIp().split(":");
					System.getProperties().setProperty("proxySet", "true");
					System.getProperties().setProperty("http.proxyHost",
							args[0]);
					System.getProperties().setProperty("http.proxyPort",
							args[1]);
				}
			}
			try {
				TimeUnit.MILLISECONDS.sleep(TIME_WAIT);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return null;
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
