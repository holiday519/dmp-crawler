package com.pxene.dmp.crawler.autocode;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;

import cn.wanghaomiao.xpath.exception.NoSuchAxisException;
import cn.wanghaomiao.xpath.exception.NoSuchFunctionException;
import cn.wanghaomiao.xpath.exception.XpathSyntaxErrorException;
import cn.wanghaomiao.xpath.model.JXDocument;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Autohome extends WebCrawler {

	private static final String REGEX = "^http://www\\.autohome\\.com\\.cn/([\\d]*)/.*";

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().toLowerCase().matches(REGEX);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		
		Pattern pattern = Pattern.compile(REGEX);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			try {
				JXDocument jxDoc = new JXDocument(Jsoup.connect(url).get());
				List<Object> names = jxDoc.sel("//div[@class='subnav-title-name']/a/allText()"); // 车名
				if (names.size() > 0) {
					String name = names.get(0).toString();
					System.out.println(name);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NoSuchAxisException e) {
				e.printStackTrace();
			} catch (NoSuchFunctionException e) {
				e.printStackTrace();
			} catch (XpathSyntaxErrorException e) {
				e.printStackTrace();
			}
		}
	}

}
