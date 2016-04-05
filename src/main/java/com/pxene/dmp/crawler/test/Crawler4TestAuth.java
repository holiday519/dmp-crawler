package com.pxene.dmp.crawler.test;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4TestAuth extends WebCrawler {

	private static final String REGEX = "^http://www\\.autohome\\.com\\.cn/[\\d]*/.*?";

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().toLowerCase().matches(REGEX);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
//		System.out.println(url);
		try {
			Document doc = Jsoup.connect(url).get();
			System.out.println(doc.select(".subnav-title-name a").html());
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

}
