package com.pxene.dmp.crawler.test;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4TestAuth extends WebCrawler {

	// 网站url（配置全站的url才能将url抓全）
	private static final String SITE_REGEX = "^http://[0-9a-zA-Z]{1,10}\\.autohome\\.com\\.cn/.*?";

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		System.out.println(url.getURL());
		return url.getURL().matches(SITE_REGEX);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		System.out.println(url);
	}

}
