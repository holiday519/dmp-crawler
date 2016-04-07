package com.pxene.dmp.crawler.autocode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.pxene.dmp.autocode.vo.BBS;
import com.pxene.dmp.autocode.vo.BuyCarEvent;
import com.pxene.dmp.common.CookieTools;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.ProxyTool;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.common.TimeConstant;
import com.pxene.dmp.crawler.test.Crawler4AutohomeUser;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Autohome extends WebCrawler {

	private static final String USERAGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	// 网站url（配置全站的url才能将url抓全）
	private static final String SITE_REGEX = "^http://.*?\\.autohome\\.com\\.cn/.*?";
	
	/**
	 * 提取style信息的url
	 */
	private static final String STYLE_REGEX = "^http://www\\.autohome\\.com\\.cn/spec/[\\d]*/$";
	
	private static final String TABLE_NAME = "t_auto_autoinfo";
	
	private static final String FAMILY_NAME = "auto_info";
	
	private static final String ROWKEY_PREFIX = "00030005_";
	
	private Log logger = LogFactory.getLog(Crawler4Autohome.class);
	
	/**
	 * 抓取BBS信息需要的常量
	 */
	private final static String REGEX4AUTO_USER = "^http://i.autohome.com.cn/([0-9]+)$";
	private final static String REGEX4AUTO_BBSMAIN = "^http://i.autohome.com.cn/([0-9]*)/club/topic$";
	private final static Gson gson = new Gson();
	private final static char SPLIT = 0x01;
	
	
	/**
	 * 抓取用户信息需要的常量
	 */
	
	/**
	 * 模拟登陆的url
	 */
	private final static String LOGIN_URL = "http://account.autohome.com.cn/Login/ValidIndex";
	private final static String LOGIN_FILE_NAME = "autohome_login.properties";
	private final static String COOKIES_FILE_NAME = "cookies.json";
	private final static String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0";

	private final static String CODE4AUTO = "00010005001";


	/**
	 * 配置文件中的cookies信息
	 */
	private Map<String, String> cookies = CookieTools.loadCookies(COOKIES_FILE_NAME);
	private HTableInterface userInfo = HBaseTools.openTable("t_auto_userinfo");
	/**
	 * 读取配置文件中的省份，城市信息
	 */
	private static JsonObject areaJson = gson.fromJson(
			new JsonReader(new InputStreamReader(Crawler4AutohomeUser.class.getResourceAsStream("/area.json"))),
			JsonObject.class);
	private static JsonArray cityList = areaJson.get("cityList").getAsJsonArray();

	
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" + "|png|tiff?|mid|mp2|mp3|mp4"  
	        + "|wav|avi|mov|mpeg|ram|m4v|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");  
	
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();  
		boolean isCar=href.matches("^http://www.autohome.com.cn/([\\d]*)/$|^http://car.autohome.com.cn/config/series/([\\d]*).html$|^http://www.autohome.com.cn/spec/([\\d]*)/$");
		return !FILTERS.matcher(href).matches() && isCar;
	}
	
	@Override
	public void visit(Page page) {
		visitSpecPage(page);
//		visitBBSPage(page);
//		visitUserPage(page);
	}

	/**
	 * 抓取汽车详情页
	 * @param page
	 */
	private void visitSpecPage(Page page) {
		String url = page.getWebURL().getURL();
		if (url.matches(STYLE_REGEX)) {
			logger.info("****"+page.getWebURL().getURL()); //日志打印
			String styleId = StringUtils.regexpExtract(url, "spec/([\\d]*)/");
			try {
				//用用戶代理
//				Map<String, String> ipInfo = ProxyTool.getIpInfo();
//		        System.getProperties().setProperty("socksProxyHost",ipInfo.get("ip"));
//		        System.getProperties().setProperty("socksProxyPort", ipInfo.get("port")); 
		        
				Document doc = Jsoup.connect(url).timeout(5000).userAgent(USERAGENT).get();
				String autoId = StringUtils.regexpExtract(doc.select(".subnav-title-return a").get(0).attr("href"), "/([\\d]*)/\\?pvareaid=");
				String styleName = doc.select(".subnav-title-name a h1").get(0).text();
				float price = Float.parseFloat(StringUtils.regexpExtract(doc.select(".cardetail-infor-price ul li").get(2).text(), "厂商指导价：([.\\d]*)万元"));
				Elements details = doc.select(".cardetail-infor-car ul li");
				float source = Float.parseFloat(StringUtils.regexpExtract(details.get(0).select("a").get(1).text(), "([.\\d]*)分"));
				String ownerOil = details.get(1).select("a").get(0).text();
				String size = details.get(2).ownText();
				String commonOil = details.get(3).ownText();
				String struct = details.get(4).ownText();
				String pqa = details.get(5).ownText();
				String engine = details.get(6).ownText();
				String gearbox = details.get(7).ownText();
				
				//提取返回的url
				String returna = doc.select("div.subnav-title-return a").first().absUrl("href");
				String autoName = Jsoup.connect(returna).userAgent(USERAGENT).timeout(5000).get().select("div.subnav-title-name a").first().text();
				
				Map<String, byte[]> datas = new HashMap<String, byte[]>();
				datas.put("style_name", Bytes.toBytes(styleName));
				datas.put("auto_name", Bytes.toBytes(autoName));
				datas.put("manu_price", Bytes.toBytes(price));
				datas.put("source", Bytes.toBytes(source));
				datas.put("owner_oil", Bytes.toBytes(ownerOil));
				datas.put("size", Bytes.toBytes(size));
				datas.put("common_oil", Bytes.toBytes(commonOil));
				datas.put("struct", Bytes.toBytes(struct));
				datas.put("pqa", Bytes.toBytes(pqa));
				datas.put("engine", Bytes.toBytes(engine));
				datas.put("gearbox", Bytes.toBytes(gearbox));
				
				HTableInterface table = HBaseTools.openTable(TABLE_NAME);
				if (table != null) {
					String rowKey = ROWKEY_PREFIX + autoId + "_" + styleId;
					HBaseTools.putColumnDatas(table, rowKey, FAMILY_NAME, datas);
					HBaseTools.closeTable(table);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 抓取用户信息的主逻辑方法
	 * 
	 * @param url用户个人主页的地址
	 */
	private void visitUserPage(Page page) {
		/**
		 * 抓取用户id
		 */
		String url = page.getWebURL().getURL();
		Pattern pattern = Pattern.compile(REGEX4AUTO_USER);
		Matcher matcher = pattern.matcher(url);
		Map<String, byte[]> family = new HashMap<>();
		String userId = url.substring(url.lastIndexOf("/") + 1, url.length());
		String rowKey = CODE4AUTO + "_" + url.substring(url.lastIndexOf("/") + 1, url.length());
		// System.out.println("rowKey:" + "==============" + rowKey);
		try {
			Document doc = Jsoup.connect(url).userAgent(USER_AGENT).timeout(5000).get();

			/**
			 * 抓取用户名称
			 */
			Elements userNameTemp = doc.getElementById("subContainer").select("h1[class]>b");
			if (userNameTemp.size() > 0) {
				String userName = userNameTemp.get(0).text();
				// System.out.println("用户名:" + "==============" + userName);
				family.put("user_name", Bytes.toBytes(userName));
			}

			/**
			 * 抓取用户性别
			 */
			Elements userSexTemp = doc.getElementById("subContainer").select("h1>span");
			if (userSexTemp.size() > 0) {
				String userSex = userSexTemp.get(0).attr("class");
				userSex = userSex.equals("man") ? "0" : "1";
				// System.out.println("性别:" + "==============" + userSex);
				family.put("sex", Bytes.toBytes(userSex));
			}

			/**
			 * 抓取用户的地址
			 */
			Elements userAdressTemp = doc.select("a[class=state-pos]");
			if (userAdressTemp.size() > 0) {
				String userAdress = userAdressTemp.get(0).text();
				// System.out.println("用户地址:" + "==============" +
				// userAdress);
				family.put("city", Bytes.toBytes(userAdress));
			}
			/**
			 * 抓取用户的生日
			 */
			Document birthDoc = Jsoup.connect(url + "/info").userAgent(USER_AGENT).timeout(5000).cookies(cookies).get();
			Element divuserinfo = birthDoc.getElementById("divuserinfo");
			if (divuserinfo == null) {
				// 说明登陆失败,更新cookie
				CookieTools.update(LOGIN_FILE_NAME, LOGIN_URL, COOKIES_FILE_NAME);
				this.cookies = CookieTools.loadCookies(COOKIES_FILE_NAME);
				birthDoc = Jsoup.connect(url + "/info").timeout(5000).userAgent(USER_AGENT).cookies(cookies).get();
				divuserinfo = birthDoc.getElementById("divuserinfo");
			}
			Elements birthTemp = divuserinfo.select("p");
			for (Element element : birthTemp) {
				if (element.select("span").text().contains("生日")) {
					String birthday = element.text().substring(element.text().indexOf("生日") + 3).trim();
					// System.out.println("用户生日：======" + birthday);
					family.put("birthday", Bytes.toBytes(birthday));
				}
			}
			/**
			 * 抓取认证汽车，关注汽车，正在使用汽车信息
			 */
			getCarsInfo(url, family);
			/**
			 * 抓取用户购买汽车行为信息
			 */
			getBuyCarsInfo(userId, family);

			// StringBuffer loginfo = new StringBuffer();
			// for (String key : family.keySet()) {
			// loginfo.append(key + "=" + Bytes.toString(family.get(key)) +
			// ", ");
			// }
			// logger.info("URL:" + url + " " + rowKey + " user_info:[" +
			// loginfo.toString() + "]");
			HBaseTools.putColumnDatas(userInfo, rowKey, "user_info", family);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 抓取用户的购买行为信息
	 * 
	 * @param url用户个人主页的地址
	 * @throws IOException
	 */
	private void getBuyCarsInfo(String userID, Map<String, byte[]> family) throws IOException {
		// 打开他的价格页
		String zhidaoUrl = "http://jiage.autohome.com.cn/web/price/otherlist?memberid=" + userID;
		Document zhidaoDoc = Jsoup.connect(zhidaoUrl).userAgent(USER_AGENT).timeout(5000).get();

		Elements buyEventLists = zhidaoDoc.select("div[class=price-i-box]>ul");

		Set<BuyCarEvent> buyCarEvents = new HashSet<BuyCarEvent>();
		if (buyEventLists.size() > 0) {
			for (Element event : buyEventLists) {
				BuyCarEvent buyCarEvent = new BuyCarEvent();
				// 表格的所有的单元格信息
				Elements carItemLists = event.select("ul[class=price-item-info fn-clear]>li");
				// 抓取车型ID
				String carStyleIdTemp = carItemLists.get(0).select("a").attr("href");
				String carStyleID = carStyleIdTemp.substring(carStyleIdTemp.indexOf("-") + 1,
						carStyleIdTemp.indexOf("#"));
				buyCarEvent.setCar(carStyleID);

				// 抓取购车价格
				String carPriceTemp = carItemLists.get(5).select("span").text();
				String carPrice = carPriceTemp.substring(0, carPriceTemp.length() - 1);
				buyCarEvent.setPrice(carPrice);
				// 抓取购车时间
				String buyCarTime = carItemLists.select("li[class=grid-2-1]").select("div[class=txcon]").text();
				try {
					buyCarTime = TimeConstant.ENGLISH_FORM4_ZHIDAOS
							.format(TimeConstant.CHINESE_FORM4_ZHIDAO.parse(buyCarTime));
					buyCarEvent.setTime(buyCarTime);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				// 抓取购车地点
				String buyCarAddrId = carItemLists.select("li[class=grid-2-2]").select("div[class=txcon addr]")
						.attr("cid");
				for (JsonElement jsonElement : cityList) {
					if (jsonElement.getAsJsonObject().get("Id").getAsString().equals(buyCarAddrId)) {
						String buyCarAddr = jsonElement.getAsJsonObject().get("Name").getAsString();
						buyCarEvent.setAddress(buyCarAddr);
					}
				}
				buyCarEvents.add(buyCarEvent);
			}
			// 打开他的口碑页

			String koubeiUrl = "http://k.autohome.com.cn/myspace/koubei/his/" + userID;
			Document koubeiDoc = Jsoup.connect(koubeiUrl).userAgent(USER_AGENT).timeout(5000).get();
			Elements koubeiLists = koubeiDoc.select("div[class=mouthcont-main]>dl");
			if (koubeiLists.size() > 0) {
				for (Element event : koubeiLists) {
					BuyCarEvent buyCarEvent = new BuyCarEvent();
					// 汽车id
					String carStyleIDTemp = event.select("li[class=info-left]").select("a[href*=spec]").attr("href");
					String carStyleID = carStyleIDTemp.substring(carStyleIDTemp.indexOf("spec") + 5,
							carStyleIDTemp.indexOf("view") - 1);
					buyCarEvent.setCar(carStyleID);

					// 价格
					String priceTemp = event.select("ul[class=mouth-info]>li").get(0).text();
					String price = priceTemp.substring(priceTemp.indexOf("：") + 1, priceTemp.length() - 2).trim();
					buyCarEvent.setPrice(price);

					// 时间
					String timeTemp = event.select("ul[class=mouth-info]>li").get(1).text();
					String time = timeTemp.substring(timeTemp.indexOf("：") + 1).trim();
					try {
						time = TimeConstant.ENGLISH_FORM4_KOUBEI.format(TimeConstant.CHINESE_FORM4_KOUBEI.parse(time));
						buyCarEvent.setTime(time);
					} catch (ParseException e) {
						e.printStackTrace();
					}

					// 地点
					String addrTemp = event.select("ul[class=mouth-info]>li").get(2).text();
					String addr = addrTemp.substring(timeTemp.indexOf("：") + 1).trim();
					buyCarEvent.setAddress(addr);
					buyCarEvents.add(buyCarEvent);
				}
			}
			// System.out.println("购车行为信息：======" + gson.toJson(buyCarEvents));
			family.put("buy_cars", Bytes.toBytes(gson.toJson(buyCarEvents)));
		}
	}

	/**
	 * 抓取认证汽车，关注汽车，正在使用汽车信息
	 * 
	 * @param url用户个人主页的地址
	 * @throws IOException
	 */
	private void getCarsInfo(String url, Map<String, byte[]> family) throws IOException {

		Document carDoc = Jsoup.connect(url + "/car").userAgent(USER_AGENT).timeout(5000).get();
		StringBuffer userAuthCarsId = new StringBuffer();
		StringBuffer userFocusCarsId = new StringBuffer();
		StringBuffer userUsingCarsId = new StringBuffer();
		while (true) {
			Elements userCarsTemp = carDoc.select("ul[class=focusCar]>li");
			if (userCarsTemp.size() > 0) {
				for (Element car : userCarsTemp) {
					/**
					 * 抓取认证的汽车
					 */
					String authCarsIdTemp = car.select("strong").attr("id");
					String authCarStyleId = authCarsIdTemp.substring(authCarsIdTemp.indexOf("_", 4) + 1);
					userAuthCarsId.append(authCarStyleId + ",");
					/**
					 * 抓取关注的汽车
					 */
					String focusCarsTemp = car.select("script").html();
					if (focusCarsTemp.contains("already")) {
						// 说明是关注的车型
						String focusCarsId = focusCarsTemp.substring(focusCarsTemp.indexOf("_") + 1,
								focusCarsTemp.lastIndexOf(")") - 1);
						userFocusCarsId.append(focusCarsId + ",");
					}
					/**
					 * 抓取正在使用的汽车
					 */
					Elements usingCarsTemp = car.select("p[class=m_t8]");
					if (usingCarsTemp.size() > 0 && usingCarsTemp.text().contains("他在开的车")) {
						String userUsingCarId = authCarStyleId;
						userUsingCarsId.append(userUsingCarId + ",");
					}
				}
			}
			// 判断有没有下一页
			Elements nextPage = carDoc.select("div[class=page paging]>a[class=next]");
			if (nextPage.size() == 0) {
				break;
			} else {
				String nextPageUrl = "http://i.autohome.com.cn" + nextPage.attr("href");
				carDoc = Jsoup.connect(nextPageUrl).userAgent(USER_AGENT).timeout(5000).get();
			}
		}
		if (userFocusCarsId.length() > 0) {
			// 说明有关注的车，需要存到数据库
			family.put("focus_car", Bytes.toBytes(userFocusCarsId.substring(0, userFocusCarsId.length() - 1)));
		}

		if (userUsingCarsId.length() > 0) {
			// 说明有正在开的车
			family.put("using_car", Bytes.toBytes(userUsingCarsId.substring(0, userUsingCarsId.length() - 1)));
		}
		if (userAuthCarsId.length() > 0) {
			// 说明有认证的车
			family.put("auth_car", Bytes.toBytes(userAuthCarsId.substring(0, userAuthCarsId.length() - 1)));
		}

	}
	
	/**
	 * 抓取BBS信息
	 * @param page
	 */
	private void visitBBSPage(Page page) {
		String url = page.getWebURL().getURL();
		List<StringBuffer> bbsInfoLists = new ArrayList<StringBuffer>();
		if (url.matches(REGEX4AUTO_BBSMAIN)) {
			BufferedWriter bw=null;
			Logger.getLogger(this.getClass()).info("URL: " + url);
			try {
				String userID = url.substring(url.indexOf("cn") + 3, url.indexOf("club") - 1);
				String pageUrl = "http://i.service.autohome.com.cn/clubapp/OtherTopic-" + userID + "-all-1.html";
				Document doc = Jsoup.connect(pageUrl).get();
				while (true) {
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
					bw = new BufferedWriter(new FileWriter("/data/user/yanghua/xu/bbsdata.txt", true));
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
			}finally{
				try {
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
