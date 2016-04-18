package com.pxene.dmp.crawler.autocode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.CjhCookieTool;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.common.UATool;
import com.pxene.dmp.constant.IPList;
import com.pxene.dmp.main.CrawlerManager;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Bitauto extends WebCrawler {
	private Log logger = LogFactory.getLog(Crawler4Bitauto.class);

	// 入库所需参数
	private static final String ROWKEY_PREFIX = "00030007_";
	private static final String TABLE_NAME = "t_auto_autoinfo2";
	private static final String FAMILY_NAME = "auto_info";

	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g"
	+ "|png|tiff?|mid|mp2|mp3|mp4" + "|wav|avi|mov|mpeg|ram|m4v|pdf"
			+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	private static final String bitauto4BBS = "^http://baa.bitauto.com/foruminterrelated/brandforumlist.html$|" 
			+ "^http://baa.bitauto.com/foruminterrelated/brandforumlist_by_tree.html?bid=[\\d]*$|" 
			+ "^http://baa.bitauto.com/[a-zA-Z1-9|-]*/$|" 
			+ "^http://baa.bitauto.com/[a-zA-Z1-9|-]*/index-all-all-[\\d]*-0.html$|"
			+ "^http://baa.bitauto.com/[a-zA-Z1-9|-]*/[a-z]*-[\\d]*\\.html$|" 
			+ "^http://i.yiche.com/.*/\\!forum/topics/$|"
			+ "^http://i.yiche.com/u[\\d]*/$";

	private static final String bitauto4Car = "^http://car.bitauto.com/$|"
			+ "^http://car.bitauto.com/tree_chexing/mb_[\\d]*/$|" 
			+ "^http://car.bitauto.com/[a-zA-Z1-9|-]*/$|" 
			+ "^http://car.bitauto.com/[a-zA-Z1-9|-]*/peizhi/$|"
			+ "^http://car.bitauto.com/[a-zA-Z1-9|-]*/m[\\d]*/$";

	private static String treeUrlBase = "http://car.bitauto.com/tree_chexing/mb_**/";
	private static String treeBBSbase = "http://baa.bitauto.com/foruminterrelated/brandforumlist_by_tree.html?bid=**";

	@Override
	protected void onContentFetchError(WebURL webUrl) {
		// TODO Auto-generated method stub

		// 设置IP代理
		logger.info("爬虫被墙....更换IP代理和UA.....");
		List<String> iplist = IPList.elements();
		String ipstr = iplist.get(new Random().nextInt(iplist.size()));
		System.getProperties().setProperty("proxySet", "true");
		System.getProperties().setProperty("http.proxyHost", ipstr.split(":")[0]);
		System.getProperties().setProperty("http.proxyPort", ipstr.split(":")[1]);

		CrawlerManager.config.setProxyHost(ipstr.split(":")[0]);
		CrawlerManager.config.setProxyPort(Integer.parseInt(ipstr.split(":")[1]));

		CrawlerManager.config.setUserAgentString(UATool.getUA());

		super.onContentFetchError(webUrl);
	}

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		return !FILTERS.matcher(href).matches() && href.matches(bitauto4BBS);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		// System.out.println(url); // 打印

//		visitUserPage(url);

		 visitSpecPage(url); //汽车信息抓取

	}

	/**
	 * 访问用户主页
	 */
	private void visitUserPage(String url) {
		// 访问论坛主页---进入各个汽车论坛的url（动态的，框架抓不全）
		if (url.matches("http://baa.bitauto.com/foruminterrelated/brandforumlist.html")) {
			for (int i = 1; i < 255; i++) {
				String carbbsurl = treeBBSbase.replace("**", i + "");
				Document doc = getDoc(carbbsurl);
				if ("社区出错页面".equals(doc.title()))
					continue;
				CrawlerManager.controller.addSeed(carbbsurl);
				// System.out.println("##BBSLIST###"+carbbsurl); //打印
			}
		}

		// 访问某一汽车的论坛
		if (url.matches("^http://baa.bitauto.com/[a-zA-Z1-9|-]*/index-all-all-[\\d]*-0.html$")) {
			String baseUrl = "http://baa.bitauto.com/ctriomphe/index-all-all-**-0.html";
			Document doc = getDoc(url);
			String maxPageNum = doc.select("div.the_pages div a.linknow").text();
			// System.out.println(maxPageNum); //打印
			if (maxPageNum.length() > 0) {
				for (int i = 1; i <= Integer.parseInt(maxPageNum); i++) {
					// System.out.println(baseUrl.replace("**", i+"")); //打印
					CrawlerManager.controller.addSeed(baseUrl.replace("**", i + ""));
				}
			}

		}

		// 访问用户主页
		if (url.matches("^http://i.yiche.com/.*/\\!forum/topics/$|^http://i.yiche.com/u[\\d]*/$")) {
			System.out.println("*****USERHOME******" + url);
			Document doc = getDoc(url);

			// 用户基本信息
			String name = doc.select("div.user_info_box b").text();
			System.out.println("name:::" + name);
			Elements lis = doc.select("ul.style_label li");
			for (Element li : lis) {
				if (li.text().length() == 0)
					continue;
				System.out.println("###" + li.text());
			}
			String bbsUrl = doc.select("div.homepage_box iframe").first().attr("src");
			visitBbsLIstPage(bbsUrl);
		}
	}

	// 访问汽车具体页面

	private void visitSpecPage(String url) {
		if (url.matches("^http://car.bitauto.com/$")) {
			for (int i = 2; i < 254; i++) {
				CrawlerManager.controller.addSeed(treeUrlBase.replace("**", i + ""));
			}
		}

		if (url.matches("^http://car.bitauto.com/tree_chexing/mb_[\\d]*/$")) {

			Document doc = getDoc(url);
			Elements brandUrls = doc.select("a[stattype=car]");
			// System.out.println(brandUrls.size());
			for (Element brandUrl : brandUrls) {
				String brand = brandUrl.absUrl("href");
				// System.out.println("######"+brand);
				CrawlerManager.controller.addSeed(brand);
			}

		}

		if (url.matches("^http://car.bitauto.com/[a-zA-Z1-9|-]*/m([\\d]*)/$")) {
			logger.info("#####URL######" + url);
			Document doc = getDoc(url);
			String priceInfo = StringUtils.regexpExtract(doc.select("div#jiaGeDetail>span.s1>em").text(), "([\\d]*|[\\d]*\\.[\\d]*)万");
			Float manu_price = new Float(0);
			if (priceInfo.length() > 0) {
				manu_price = Float.parseFloat(priceInfo);
			}
			// System.out.println("manu_price:::"+manu_price);
			Elements refers = doc.select("ul.ul-set span");
			String com_wear = "";
			if (refers.size() - 1 > 0) {
				com_wear = refers.get(0).text();
			}

			// System.out.println("com_wear:::"+com_wear);
			String gearbox = "";
			if (refers.size() - 1 > 1) {
				gearbox = refers.get(1).text();
			}
			// System.out.println("gearbox:::"+gearbox);
			String cc = "";
			if (refers.size() - 1 > 2) {
				cc = refers.get(0).text();
			}
			// System.out.println("cc:::"+cc);
			String engine = "";
			if (refers.size() - 1 > 3) {
				engine = refers.get(3).text();
			}
			// System.out.println("engine:::"+engine);
			Elements pqaInfo = doc.select("div[class=car_config car_top_set] table tbody td[class=td-b-sty td-p-sty]");
			String pqa = "";
			if (pqaInfo.size() > 0) {
				pqa = pqaInfo.first().text();
			}

			// System.out.println("qpa:::"+pqa);

			String brandId = StringUtils.regexpExtract(url, "m([\\d]*)/$");
			System.out.println("brandId:::" + brandId);
			String carId = StringUtils.regexpExtract(url, "m([\\d]*)/$");
			// System.out.println("carId:::"+carId);
			Elements nameEle = doc.select("div[class=car_navigate] a");
			StringBuffer nameBuffer = new StringBuffer();
			for (int i = 0; i < nameEle.size(); i++) {
				if (i < 2)
					continue;
				nameBuffer.append(nameEle.get(i).text()).append("#");
			}
			String name = doc.select("div.car_navigate strong").text();
			if (nameBuffer.length() > 1) {
				name = nameBuffer.substring(0, nameBuffer.length() - 1) + "#" + name;
				// System.out.println("name:::"+name);
			}

			// 入库
			Map<String, byte[]> datas = new HashMap<String, byte[]>();
			datas.put("auto_name", Bytes.toBytes(name));
			datas.put("manu_price", Bytes.toBytes(manu_price));
			datas.put("com_wear", Bytes.toBytes(com_wear));
			datas.put("pqa", Bytes.toBytes(pqa));
			datas.put("engine", Bytes.toBytes(engine));
			datas.put("gearbox", Bytes.toBytes(gearbox));
			datas.put("cc", Bytes.toBytes(cc)); // 排量

			HTableInterface table = HBaseTools.openTable(TABLE_NAME);
			if (table != null) {
				String rowKey = ROWKEY_PREFIX + brandId + "_" + carId;
				// HBaseTools.putColumnDatas(table, rowKey, FAMILY_NAME, datas);
				HBaseTools.closeTable(table);
			}
		}
	}

	/**
	 * 抓取BBS列表页
	 * 
	 * @param url
	 */
	public void visitBbsLIstPage(String url) {
		System.out.println("#####BBSLIST########" + url);
		Document doc = getDoc(url);

		// 分为没有分页的和有分页的两种
		if (doc.select("span#Pager div.the_pages div a").text().length()==0 ) {
			Elements bss = doc.select("div.line_box div.postslist_xh");
			for (Element bs : bss) {
				String bbsPage = bs.select("ul li.bt a").attr("href");
				System.out.println(bbsPage);
				visitbbsPage(bbsPage);
			}
		} else {
			int max = Integer.parseInt(doc.select("span#Pager div.the_pages div a").last().previousElementSibling().text());
			for (int i = 1; i <= max; i++) {
				String eachUrl = url.replace(".html", "-" + i + ".html");
				System.out.println("BBBB****" + eachUrl);
				doc = getDoc(eachUrl);
				Elements bss = doc.select("div.line_box div.postslist_xh");
				for (Element bs : bss) {
					String bbsPage = bs.select("ul li.bt a").attr("href");
					System.out.println(bbsPage);
					visitbbsPage(bbsPage);
				}
			}
		}

	}

	/**
	 * 单个帖子
	 * 
	 * @param url
	 */
	public void visitbbsPage(String url) {
		System.out.println("###########BBS##########" + url);
		Document doc = getDoc(url);
		String title = doc.select("div.title_box h1").text();
		System.out.println("title:::" + title);
		String content = doc.select("div.post_width").text();
		System.out.println("content:::" + content);
	}

	// 确保document一定获取成功
	private Document getDoc(String url) {
		Document doc = null;
		int i = 0;
		while (true) {
			try {
				doc = Jsoup.connect(url).cookies(CjhCookieTool.getCookie("autohome")).userAgent(UATool.getUA()).get();
				break;
			} catch (Exception e) {
				logger.info("抓取失败。。。重来。。。");
				List<String> iplist = IPList.elements();
				String ipstr = iplist.get(new Random().nextInt(iplist.size()));
				System.getProperties().setProperty("proxySet", "true");
				System.getProperties().setProperty("http.proxyHost", ipstr.split(":")[0]);
				System.getProperties().setProperty("http.proxyPort", ipstr.split(":")[1]);
				i++;
			}

			// 防止过多次的死循环
			if (i >= 20) {
				break;
			}

		}
		return doc;
	}

}
