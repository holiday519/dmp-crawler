package com.pxene.dmp;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class MyCrawler extends WebCrawler {

	private final static String REGEX = "^http://www\\.autohome\\.com\\.cn/([\\d]*)/.*$";

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().toLowerCase().matches(REGEX);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		//System.out.println("URL: " + url);
		// 获取code
		Pattern pattern = Pattern.compile(REGEX);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			String autoCode = matcher.group(1);
			// 解析网页
			try {
				Document doc = Jsoup.connect(url).get();
				String autoName = doc.select(".subnav-title-name a").html().replaceAll("<.*?>", "");
				System.out.println(autoCode + ":::::" + autoName);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
