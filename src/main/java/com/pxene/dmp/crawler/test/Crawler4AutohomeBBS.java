package com.pxene.dmp.crawler.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.pxene.dmp.autocode.vo.BBS;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4AutohomeBBS extends WebCrawler {
	private final static String REGEX4AUTO_USER_NEXTPAGE = "^http://club.autohome.com.cn/bbs/forum-(a|c|o)-([0-9]*)-([0-9]*)(\\.)html(\\?)qatype=-1$";
	private final static String REGEX4AUTO_USER_CARLIST = "^http://club.autohome.com.cn/bbs/forum-c-([0-9]*)-1.html$";

	private final static String REGEX4AUTO_USER_BBS = "^http://club.autohome.com.cn/bbs/(thread|threadqa)-(a|c|o)-([0-9]*)-([0-9]*)-([0-9]*).html$";

	private final static String REGEX4AUTO_USER = "^http://i.autohome.com.cn/([0-9]+)$";
	// private final static String REGEX4AUTO_BBSMAIN =
	// "^http://i.autohome.com.cn/([0-9]*)/club/topic$";
	private final static Gson gson = new Gson();
	private final static char SPLIT = 0x01;
	private static BufferedWriter bw = null;
	static {
		try {
			bw = new BufferedWriter(new FileWriter("/data/user/yanghua/xu/bbsdata.txt", true));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		boolean isBBS = href.matches(REGEX4AUTO_USER) || href.matches(REGEX4AUTO_USER_CARLIST)
				|| href.matches(REGEX4AUTO_USER_BBS) || href.matches(REGEX4AUTO_USER_NEXTPAGE);
		return isBBS;
	}

	@Override
	public void visit(Page page) {
		visitBBSPage(page);
	}

	/**
	 * 抓取BBS信息
	 * 
	 * @param page
	 */
	private void visitBBSPage(Page page) {
		String url = page.getWebURL().getURL();
		// System.out.println(url);
		if (url.matches(REGEX4AUTO_USER)) {
			Logger.getLogger(this.getClass()).info("URL: " + url);
			try {
				String userID = url.substring(url.lastIndexOf("/") + 1, url.length());
				String pageUrl = "http://i.service.autohome.com.cn/clubapp/OtherTopic-" + userID + "-all-1.html";
				Document doc = Jsoup.connect(pageUrl).get();
				List<StringBuffer> bbsInfoLists = null;
				while (true) {
					bbsInfoLists = new ArrayList<StringBuffer>();
					Elements bbsLists = doc.select("table[class=topicList]>tbody>tr");
					for (Element bbs : bbsLists) {
						StringBuffer bbsInfo = new StringBuffer();
						Elements titleTemp = bbs.select("div[class=topicTitle]>p");
						if (titleTemp.size() == 0) {
							continue;
						}
						// 帖子唯一标识
						String bbsidTemp = titleTemp.get(0).select("a").attr("href");
						String bbsid = bbsidTemp.substring(bbsidTemp.indexOf("thread") + 7, bbsidTemp.lastIndexOf("-"));
						bbsInfo.append(bbsid + SPLIT + "" + userID);
						// 标题
						String title = titleTemp.get(0).select("a").text();
						bbsInfo.append(SPLIT + "" + title);
						// 来自那个论坛
						String fromBBS = titleTemp.get(1).select("a").text();
						bbsInfo.append(SPLIT + "" + fromBBS);
						// 点击数,回复数
						// 返回一个json串
						Document replyDoc = Jsoup.connect("http://i.service.autohome.com.cn/clubapp/rv?ids="
								+ bbsid.substring(bbsid.lastIndexOf("-") + 1)).get();
						BBS replys_views = gson.fromJson(replyDoc.text().substring(1, replyDoc.text().length() - 1),
								BBS.class);
						bbsInfo.append(SPLIT + "" + replys_views.getReplys() + SPLIT + "" + replys_views.getViews());
						// 发表时间
						String time = bbs.select("td[class=txtCen]").get(1).text();
						bbsInfo.append(SPLIT + "" + time.trim());
						bbsInfoLists.add(bbsInfo);
					}
					// 写入文件
					for (StringBuffer element : bbsInfoLists) {
						bw.write(element.toString());
						bw.newLine();
						bw.flush();
					}
					// 判断有没有下一页
					Elements nextPage = doc.select("div[class=paging]>a[class=next]");
					if (nextPage.size() == 0) {
						// 跳出无限循环
						break;
					}
					doc = Jsoup.connect("http://i.service.autohome.com.cn" + nextPage.attr("href")).get();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
