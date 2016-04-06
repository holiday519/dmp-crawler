package com.pxene.dmp.crawler.autocode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.pxene.dmp.domain.BBS;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4AutohomeBBS extends WebCrawler {
	private final static String REGEX4AUTO_USER = "^http://i.autohome.com.cn/([0-9]+)$";
	private final static String REGEX4AUTO_BBSMAIN = "^http://i.autohome.com.cn/([0-9]*)/club/topic$";
	private final static Gson gson = new Gson();
	private final static List<StringBuffer> bbsInfoLists = new ArrayList<StringBuffer>();
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
		String urlStr = url.getURL().toLowerCase();
		return urlStr.matches(REGEX4AUTO_BBSMAIN) || urlStr.matches(REGEX4AUTO_USER);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		if (url.matches(REGEX4AUTO_BBSMAIN)) {
			try {
				String userID = url.substring(url.indexOf("cn") + 3, url.indexOf("club") - 1);
				System.out.println(userID);
				String pageUrl = "http://i.service.autohome.com.cn/clubapp/OtherTopic-" + "5265908" + "-all-1.html";
				Document doc = Jsoup.connect(pageUrl).get();
				while (true) {
					Elements bbsLists = doc.select("table[class=topicList]>tbody>tr");
					System.out.println(bbsLists.size());
					for (Element bbs : bbsLists) {
						StringBuffer bbsInfo = new StringBuffer();
						Elements titleTemp = bbs.select("div[class=topicTitle]>p");
						if (titleTemp.size() == 0) {
							continue;
						}
						// 帖子唯一标识
						String bbsidTemp = titleTemp.get(0).select("a").attr("href");
						String bbsid = bbsidTemp.substring(bbsidTemp.indexOf("thread") + 7, bbsidTemp.lastIndexOf("-"));
						bbsInfo.append(bbsid + "\0x01" + userID);
						// 标题
						String title = titleTemp.get(0).select("a").text();
						bbsInfo.append("\0x01" + title);
						// 来自那个论坛
						String fromBBS = titleTemp.get(1).select("a").text();
						bbsInfo.append("\0x01" + fromBBS);
						// 点击数,回复数
						// 返回一个json串
						Document replyDoc = Jsoup.connect("http://i.service.autohome.com.cn/clubapp/rv?ids="
								+ bbsid.substring(bbsid.lastIndexOf("-") + 1)).get();
						BBS replys_views = gson.fromJson(replyDoc.text().substring(1, replyDoc.text().length() - 1),
								BBS.class);
						bbsInfo.append("\0x01" + replys_views.getReplys() + "\0x01" + replys_views.getViews());
						// 发表时间
						String time = bbs.select("td[class=txtCen]").get(1).text();
						bbsInfo.append("\0x01" + time.trim());
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
	};
}
