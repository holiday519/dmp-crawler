package com.pxene.dmp.crawler.autocode;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.pxene.dmp.common.CookieTools;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.TimeConstant;
import com.pxene.dmp.domain.BuyCarEvent;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4AutohomeUser extends WebCrawler {
	/**
	 * 用户个人主页url匹配规则
	 */
	private final static String REGEX4AUTO_USER = "^http://i.autohome.com.cn/([0-9]+)$";
	/**
	 * 模拟登陆的url
	 */
	private final static String LOGIN_URL = "http://account.autohome.com.cn/Login/ValidIndex";
	private final static String LOGIN_FILE_NAME = "autohome_login.properties";
	private final static String COOKIES_FILE_NAME = "cookies.json";
	private final static String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0";

	private final static String CODE4AUTO = "00010005001";

	private final static Gson gson = new Gson();

	private final static Logger logger = Logger.getLogger(Crawler4AutohomeUser.class);
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

	@Override
	public boolean shouldVisit(Page page, WebURL url) {
		String href = url.getURL().toLowerCase();
		return href.matches(REGEX4AUTO_USER);
	}

	@Override
	public void visit(Page page) {
		Map<String, Map<String, byte[]>> data = new HashMap<>();
		Map<String, byte[]> family = new HashMap<>();
		String url = page.getWebURL().getURL();
		logger.info("URL: " + url);
		Pattern pattern = Pattern.compile(REGEX4AUTO_USER);
		Matcher matcher = pattern.matcher(url);

		if (matcher.find()) {
			/**
			 * 抓取用户id
			 */
			String userId = url.substring(url.lastIndexOf("/") + 1, url.length());
			System.out.println(userId);
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
				Document birthDoc = Jsoup.connect(url + "/info").userAgent(USER_AGENT).timeout(5000).cookies(cookies)
						.get();
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

				data.put("user_info", family);
				// StringBuffer loginfo = new StringBuffer();
				// for (String key : family.keySet()) {
				// loginfo.append(key + "=" + Bytes.toString(family.get(key)) +
				// ", ");
				// }
				// logger.info("URL:" + url + " " + rowKey + " user_info:[" +
				// loginfo.toString() + "]");
				HBaseTools.putData(userInfo, rowKey, data);
			} catch (IOException e) {
				e.printStackTrace();
			}
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
			family.put("auth_car", Bytes.toBytes(userAuthCarsId.substring(0, userAuthCarsId.length() - 1)));
		}

	}
}
