package com.pxene.dmp.crawler.auto;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pxene.dmp.common.AjaxClient;
import com.pxene.dmp.common.CookieList;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Autohome extends BaseCrawler {

	private Log log = LogFactory.getLog(Crawler4Autohome.class);
	private static Map<String, String> cookie;
	
	// hbase参数
	private static final String AUTO_INFO_TABLE = "t_auto_autoinfo";
	private static final String AUTO_INFO_FAMILY = "auto_info";
	
	private static final String USER_INFO_TABLE = "t_auto_userinfo";
	private static final String USER_INFO_FAMILY = "user_info";
	
	private static final String POST_INFO_TABLE = "t_auto_postinfo";
	private static final String POST_INFO_FAMILY = "post_info";
	
	private static final String DEALER_INFO_TABLE = "t_auto_dealerinfo";
	private static final String DEALER_INFO_FAMILY = "dealer_info";

	private static final String ROWKEY_PREFIX = "00030005_";

	// 正则
	private final static String FILTER_REGEX = ".*(\\.(css|js|bmp|gif|jpe?g"
			+ "|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|pdf|rm|smil|wmv|swf|wma|zip|rar|gz))$";
	private class AUTO_INFO_REGEXS {
		public static final String SALE_AUTO = "^http://www\\.autohome\\.com\\.cn/[0-9]+/.*$";
		public static final String UNSALE_AUTO = "^http://www\\.autohome\\.com\\.cn/[0-9]+/sale\\.html$";
		public static final String STYLE_LIST = "^http://car\\.autohome\\.com\\.cn/config/series/[0-9\\-]+\\.html$";
	}
	private class POST_INFO_REGEXS {
		public static final String BBS_LIST = "^http://club\\.autohome\\.com\\.cn/bbs/forum-(a|c|o)-[0-9]+-[0-9]+\\.html.*$";
		public static final String BRAND_LIST = "^http://club\\.autohome\\.com\\.cn/bbs/brand-[0-9]+-c-[0-9]+-[0-9]+\\.html.*$";
		public static final String ZHIDAO_LIST = "^http://zhidao\\.autohome\\.com\\.cn/list/s1-[0-9]+\\.html$";
		public static final String POST_DETAIL = "^http://club\\.autohome\\.com\\.cn/bbs/(thread|threadqa)-(a|c|o)-[0-9]+-[0-9]+-[0-9]+\\.html$";
	}
	private class USER_INFO_REGEXS {
		public static final String WILD_CARD = "^http://i\\.autohome\\.com\\.cn/[0-9]+.*$|^http://jiage\\.autohome\\.com\\.cn/web/price/otherlist\\?memberid=[0-9]+.*$";
		public static final String HOME_PAGE = "^http://i\\.autohome\\.com\\.cn/[0-9]+$";
		public static final String BASE_INFO = "^http://i\\.autohome\\.com\\.cn/[0-9]+/info$";
		public static final String FOLLOWING = "^http://i\\.autohome\\.com\\.cn/[0-9]+/following(?!\\?page).*$";
		public static final String FOLLOWERS = "^http://i\\.autohome\\.com\\.cn/[0-9]+/followers(?!\\?page).*$";
		public static final String CAR = "^http://i\\.autohome\\.com\\.cn/[0-9]+/car(?!\\?page).*$";
		//public static final String PRICE = "^http://i\\.autohome\\.com\\.cn/[0-9]+/price.*$";
		public static final String PRICE = "^http://jiage\\.autohome\\.com\\.cn/web/price/otherlist\\?memberid=[0-9]+&(?!pageindex).*$";
	}
	private static final String DEALER_INFO_REGEXS = "^http://dealer\\.autohome\\.com\\.cn/china/0_0_0_0_[0-9]+_[12]_0\\.html$"; 
	
	private class AJAX_URL_TMPL {
		public static final String USER_INFO = "http://i.autohome.com.cn/ajax/home/GetUserInfo?userid={userid}&r={random}&_={timestamp}";
		public static final String USER_INFO_REFERER = "http://i.autohome.com.cn/{userid}";
		public static final String USER_FOLLOWING_PAGE = "http://i.autohome.com.cn/{userid}/following?page={pageno}";
		public static final String USER_FOLLOWERS_PAGE = "http://i.autohome.com.cn/{userid}/followers?page={pageno}";
		public static final String USER_CAR_PAGE = "http://i.autohome.com.cn/{userid}/car?page={pageno}";
		public static final String USER_PRICE_PAGE = "http://jiage.autohome.com.cn/web/price/otherlist?memberid={userid}&pageindex={pageno}";
	}
	
	private static final String BUY_INFO_TMPL = "{car:'{car}',price:'{price}',time:'{time}',address:'{address}',dealer:'{dealer}'}";
	
	// json解析
	private static JsonParser parser = new JsonParser();

	public Crawler4Autohome() {
		super("/" + Crawler4Autohome.class.getName().replace(".", "/") + ".json");
		// 将cookie存入静态变量
		String url = loginConf.getUrl();
		String usrkey = loginConf.getUsrkey();
		String username = loginConf.getUsername();
		String pwdkey = loginConf.getPwdkey();
		String password = loginConf.getPassword();
		cookie = CookieList.get(url, usrkey, username, pwdkey, password);
	}

	@Override
	public boolean shouldVisit(Page referringPage, WebURL webURL) {
		String url = webURL.getURL().toLowerCase();
		if (url.matches(FILTER_REGEX)) {
			return false;
		}
		// autoinfo
		if (url.matches(AUTO_INFO_REGEXS.SALE_AUTO)
				|| url.matches(AUTO_INFO_REGEXS.UNSALE_AUTO)
				|| url.matches(AUTO_INFO_REGEXS.STYLE_LIST)) {
			return true;
		}
		// userinfo + postinfo
		if (url.matches(POST_INFO_REGEXS.BBS_LIST)
				|| url.matches(POST_INFO_REGEXS.BRAND_LIST)
				|| url.matches(POST_INFO_REGEXS.ZHIDAO_LIST)
				|| url.matches(POST_INFO_REGEXS.POST_DETAIL)
				|| url.matches(USER_INFO_REGEXS.WILD_CARD)) {
			return true;
		}
		// dealerinfo
		if (url.matches(DEALER_INFO_REGEXS)) {
			return true;
		}
		
		return false;
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		log.info("<=ee-debug=>visit:" + url);
		if (url.matches(AUTO_INFO_REGEXS.STYLE_LIST)) {
			getAutoInfo(url, ((HtmlParseData) page.getParseData()).getHtml());
		}
		if (url.matches(USER_INFO_REGEXS.WILD_CARD)) {
			getUserInfo(url, ((HtmlParseData) page.getParseData()).getHtml());
		}
		if (url.matches(POST_INFO_REGEXS.POST_DETAIL)) {
			getPostInfo(url, ((HtmlParseData) page.getParseData()).getHtml());
		}
		if (url.matches(DEALER_INFO_REGEXS)) {
			getDealerInfo(url, ((HtmlParseData) page.getParseData()).getHtml());
		}
	}

	private void getAutoInfo(String url, String html) {
		Document doc = parseHtml(html);
		if (doc == null) {
			return;
		}
		log.info("<=ee-debug=>getAutoInfo:" + url);
		Elements scripts = doc.select("script");
		for (Element script : scripts) {
			String content = script.html();
			if (content.contains("var config =")) {
				String jStr = StringUtils.regexpExtract(content, "var config = (\\{.*?\\});").trim();
				if (jStr.length() > 0) {
					Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
					// 解析json
					JsonObject jObj = parser.parse(jStr).getAsJsonObject();
					JsonObject result = jObj.get("result").getAsJsonObject();
					// 
					String autoId = result.get("seriesid").getAsString();
					String prefix = ROWKEY_PREFIX + autoId + "_";
					// 
					JsonArray params = result.get("paramtypeitems").getAsJsonArray().get(0).getAsJsonObject().get("paramitems").getAsJsonArray();
					for (JsonElement param : params) {
						String paramName = param.getAsJsonObject().get("name").getAsString();
						JsonArray paramVals = param.getAsJsonObject().get("valueitems").getAsJsonArray();
						
						switch (paramName) {
						case "车型名称":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String style = val.getAsJsonObject().get("value").getAsString();
								System.out.println(styleId + ":" + style);
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "style", Bytes.toBytes(style));
							}
							break;
						case "厂商指导价(元)":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String price = StringUtils.regexpExtract(val.getAsJsonObject().get("value").getAsString(), "([.\\d]+)万");
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "price", Bytes.toBytes(price));
							}
							break;
						case "级别":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String level = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "level", Bytes.toBytes(level));
							}
							break;	
						case "发动机":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String engine = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "engine", Bytes.toBytes(engine));
							}
							break;
						case "变速箱":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String gearbox = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "gearbox", Bytes.toBytes(gearbox));
							}
							break;
						case "长*宽*高(mm)":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String size = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "size", Bytes.toBytes(size));
							}
							break;
						case "车身结构":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String struct = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "struct", Bytes.toBytes(struct));
							}
							break;
						case "最高车速(km/h)":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String speed = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "speed", Bytes.toBytes(speed));
							}
							break;
						case "工信部综合油耗(L/100km)":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String fuel = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "fuel", Bytes.toBytes(fuel));
							}
							break;
						case "整车质保":
							for (JsonElement val : paramVals) {
								String styleId = val.getAsJsonObject().get("specid").getAsString();
								String pqa = val.getAsJsonObject().get("value").getAsString();
								insertData(rowDatas, prefix + styleId, AUTO_INFO_FAMILY, "pqa", Bytes.toBytes(pqa));
							}
							break;
						default:
							break;
						}
					}
					// 抓取车名
					String name = doc.select("div.subnav-title-name a").first().text();
					for (Map.Entry<String, Map<String, Map<String, byte[]>>> rowData : rowDatas.entrySet()) {
						insertData(rowDatas, rowData.getKey(), AUTO_INFO_FAMILY, "name", Bytes.toBytes(name));
					}
					
					if (rowDatas.size() > 0) {
						Table table = HBaseTools.openTable(AUTO_INFO_TABLE);
						if (table != null) {
							HBaseTools.putRowDatas(table, rowDatas);
							HBaseTools.closeTable(table);
						}
					}
				}
			}
		}
	}
	
	private void getUserInfo(String url, String html) {
		Document doc = parseHtml(html);
		if (doc == null) {
			return;
		}
		log.info("<=ee-debug=>getUserInfo:" + url);
		// 获取userId
		Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
		String userId = StringUtils.regexpExtract(url, "cn/([0-9]+)");
		String rowKey = ROWKEY_PREFIX + userId;
		// 首页
		if (url.matches(USER_INFO_REGEXS.HOME_PAGE)) {
			// 用户等级
			String userUrl = AJAX_URL_TMPL.USER_INFO.replace("{userid}", userId)
					.replace("{random}", String.valueOf(Math.random()))
					.replace("{timestamp}", String.valueOf((new Date()).getTime()));
			String userReferer = AJAX_URL_TMPL.USER_INFO_REFERER.replace("{userid}", userId);
			Map<String, String> userHeaders = new HashMap<String, String>();
			userHeaders.put("Referer", userReferer);
			AjaxClient client = new AjaxClient.Builder(userUrl, userHeaders).build();
			JsonObject result = client.execute();
			client.close();
			String level = result.get("UserGrade").getAsJsonObject().get("Grade").getAsString();
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "level", Bytes.toBytes(level));
			// 是否是vip
			String vip = "1";
			Element i = doc.select("i.icon16-vorange").get(0);
			if (i.attr("style").contains("display:none")) {
				vip = "0";
			}
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "vip", Bytes.toBytes(vip));
		}
		// 基本资料
		if (url.matches(USER_INFO_REGEXS.BASE_INFO)) {
			Element div = doc.select("#divuserinfo").get(0);
			String name = div.child(0).ownText();
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "name", Bytes.toBytes(name));
			String sex = div.child(1).ownText().contains("男") ? "0" : "1";
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "sex", Bytes.toBytes(sex));
			String city = div.child(3).ownText();
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "city", Bytes.toBytes(city));
		}
		// 他的关注
		if (url.matches(USER_INFO_REGEXS.FOLLOWING)) {
			Set<String> codes = new HashSet<String>();
			Element p = doc.select("#dynamic .subdyn2").get(0);
			int amount = 0;
			String str = StringUtils.regexpExtract(p.text(), "关注([\\d]+)人");
			if (str.length() > 0) {
				amount = Integer.parseInt(str);
			}
			int pageSize = amount / 20 + 1;
			for (int pageNo=1; pageNo<=pageSize; pageNo++) {
				String pageUrl = AJAX_URL_TMPL.USER_FOLLOWING_PAGE
						.replace("{userid}", userId)
						.replace("{pageno}", String.valueOf(pageNo));
				Document pageDoc = connectUrl(pageUrl, cookie);
				Elements lis = pageDoc.select("#ulList li");
				for (Element li : lis) {
					codes.add(li.attr("id"));
				}
			}
			String following = org.apache.commons.lang.StringUtils.join(codes, ",");
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "following", Bytes.toBytes(following));
		}
		// 他的粉丝
		if (url.matches(USER_INFO_REGEXS.FOLLOWERS)) {
			Set<String> codes = new HashSet<String>();
			Element p = doc.select("#dynamic .subdyn2").get(0);
			int amount = 0;
			String str = StringUtils.regexpExtract(p.text(), "已有([\\d]+)人");
			if (str.length() > 0) {
				amount = Integer.parseInt(str);
			}
			int pageSize = amount / 20 + 1;
			for (int pageNo=1; pageNo<=pageSize; pageNo++) {
				String pageUrl = AJAX_URL_TMPL.USER_FOLLOWERS_PAGE
						.replace("{userid}", userId)
						.replace("{pageno}", String.valueOf(pageNo));
				Document pageDoc = connectUrl(pageUrl, cookie);
				Elements lis = pageDoc.select("#ulList li");
				for (Element li : lis) {
					codes.add(li.attr("id"));
				}
			}
			String followers = org.apache.commons.lang.StringUtils.join(codes, ",");
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "followers", Bytes.toBytes(followers));
		}
		// 认证的车
		if (url.matches(USER_INFO_REGEXS.CAR)) {
			StringBuilder sb = new StringBuilder();
			int amount = Integer.parseInt(doc.select("span.fcolor_6").get(0).text());
			int pageSize = amount / 10 + 1;
			for (int pageNo=1; pageNo<=pageSize; pageNo++) {
				String pageUrl = AJAX_URL_TMPL.USER_CAR_PAGE
						.replace("{userid}", userId)
						.replace("{pageno}", String.valueOf(pageNo));
				Document pageDoc = connectUrl(pageUrl);
				Elements lis = pageDoc.select("#dynamic .focusCar li");
				for (Element li : lis) {
					Element div = li.select("div.fcpc").get(0);
					String code = StringUtils.regexpExtract(div.select("strong").get(0).attr("id"), "^bsp_[0-9]+_([0-9]+_[0-9]+)$");
					String flag = "";
					// 是否认证
					// 是否认证
					if (!div.select("a.rzcz").get(0).hasAttr("style")) {
						flag += 1;
					}
					String str = div.select("p").get(0).text();
					// 是否关注
					if (str.contains("关注的")) {
						flag += 2;
					}
					// 是否正在开
					if (str.contains("开的")) {
						flag += 3;
					}
					if (flag.length() > 0) {
						code = code + "(" + flag + ")";
					}
					sb.append(code).append(",");
				}
				insertData(rowDatas, rowKey, USER_INFO_FAMILY, "cars", Bytes.toBytes(sb.substring(0, sb.length()-1)));
			}
		}
		// 购买信息
		if (url.matches(USER_INFO_REGEXS.PRICE)) {
			Elements spans = doc.select("span.page-item-info");
			if (spans.size() > 0) {
				StringBuilder sb = new StringBuilder();
				int pageSize = Integer.parseInt(StringUtils.regexpExtract(spans.get(0).text(), "共([0-9]+)页"));
				for (int pageNo=1; pageNo<=pageSize; pageNo++) {
					String pageUrl = AJAX_URL_TMPL.USER_PRICE_PAGE
							.replace("{userid}", userId)
							.replace("{pageno}", String.valueOf(pageNo));
					Document pageDoc = connectUrl(pageUrl);
					// 一个页面上最多有两条购车信息
					Elements lis = pageDoc.select("ul.price-list li.price-item");
					for (Element li : lis) {
						String car = StringUtils.regexpExtract(
								li.getElementsContainingOwnText("购买车型").get(0).nextElementSibling().select("a").get(0).attr("href"), 
								"p-([0-9]+)#");
						String price = StringUtils.regexpExtract(
								li.getElementsContainingOwnText("裸车价").get(0).parent().nextElementSibling().select("span.price-desc").get(0).text(), 
								"([.\\d]+)万");
						String time = li.getElementsContainingOwnText("购车时间").get(0).nextElementSibling().text().replaceAll("[^0-9]", "");
						Element div = li.getElementsContainingOwnText("购车地点").get(0).nextElementSibling();
						String province = div.attr("pid");
						String city = div.attr("cid");
						Elements as = li.getElementsContainingOwnText("购买商家").get(0).nextElementSibling().select("a.title");
						String dealer = "0";
						if (as.size() > 0) {
							dealer = StringUtils.regexpExtract(as.get(0).attr("href"), "cn/([0-9]+)#");
						}
						String buyInfo = BUY_INFO_TMPL.replace("{car}", car).replace("{price}", price).replace("{time}", time)
								.replace("{address}", province+","+city).replace("{dealer}", dealer);
						sb.append(buyInfo).append(",");
					}
				}
				insertData(rowDatas, rowKey, USER_INFO_FAMILY, "buy_info", Bytes.toBytes("["+sb.substring(0, sb.length()-1)+"]"));
			}
		}
		
		if (rowDatas.size() > 0) {
			Table table = HBaseTools.openTable(USER_INFO_TABLE);
			if (table != null) {
				HBaseTools.putRowDatas(table, rowDatas);
				HBaseTools.closeTable(table);
			}
		}
	}
	
	private void getPostInfo(String url, String html) {
		Document doc = parseHtml(html);
		if (doc == null) {
			return;
		}
		log.info("<=ee-debug=>getPostInfo:" + url);
		// 帖子
		if (url.matches(POST_INFO_REGEXS.POST_DETAIL)) {
			Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
			Element post = doc.select("#F0").get(0);
			String userId = post.attr("uid");
			// 发帖时间（精确到秒）
			String time = doc.getElementsByAttributeValue("xname", "date").get(0).text().replaceAll("[^0-9]", "");
			String rowKey = ROWKEY_PREFIX + userId + "_" + time;
			
			String bbsId = StringUtils.regexpExtract(url, "-([0-9]+)-[0-9]+-[0-9]+\\.html");
			insertData(rowDatas, rowKey, POST_INFO_FAMILY, "bbs_id", Bytes.toBytes(bbsId));
			String bbsName = doc.select("#a_bbsname").get(0).text();
			insertData(rowDatas, rowKey, POST_INFO_FAMILY, "bbs_name", Bytes.toBytes(bbsName));
			String postId = StringUtils.regexpExtract(url, "-[0-9]+-([0-9]+)-[0-9]+\\.html");
			insertData(rowDatas, rowKey, POST_INFO_FAMILY, "post_id", Bytes.toBytes(postId));
			String title = doc.select("h1.rtitle").get(0).text();
			insertData(rowDatas, rowKey, POST_INFO_FAMILY, "post_title", Bytes.toBytes(title));
			String context = doc.select("div.conttxt").get(0).text();
			insertData(rowDatas, rowKey, POST_INFO_FAMILY, "post_content", Bytes.toBytes(context));
			
			if (rowDatas.size() > 0) {
				Table table = HBaseTools.openTable(POST_INFO_TABLE);
				if (table != null) {
					HBaseTools.putRowDatas(table, rowDatas);
					HBaseTools.closeTable(table);
				}
			}
		}
	}
	
	private void getDealerInfo(String url, String html) {
		Document doc = parseHtml(html);
		if (doc == null) {
			return;
		}
		log.info("<=ee-debug=>getDealerInfo:" + url);
		if (url.matches(DEALER_INFO_REGEXS)) {
			Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
			Elements scripts = doc.select("script");
			Map<String, String> locs = new HashMap<String, String>();
			for (Element script : scripts) {
				String content = script.html();
				if (content.contains("data=")) {
					String jStr = StringUtils.regexpExtract(content, "data=(\\[.*?\\])").trim();
					if (jStr.length() > 0) {
						// 解析json
						JsonArray dealers = parser.parse(jStr).getAsJsonArray();
						for (JsonElement dealer : dealers) {
							JsonObject jObj = dealer.getAsJsonObject();
							String dealerId = StringUtils.regexpExtract(jObj.get("url").getAsString(), "cn/([0-9]+)/index");
							String latlon = jObj.get("latlon").getAsString();
							locs.put(dealerId, latlon);
						}
					}
				}
			}
			Elements divs = doc.select("div.dealer-cont");
			for (Element div : divs) {
				Element a = div.select("a.btn-map").get(0);
				String id = a.attr("js-did");
				String rowKey = ROWKEY_PREFIX + id;
				String name = a.attr("js-dname");
				insertData(rowDatas, rowKey, DEALER_INFO_FAMILY, "name", Bytes.toBytes(name));
				String brand = a.attr("js-dbrand");
				insertData(rowDatas, rowKey, DEALER_INFO_FAMILY, "brand", Bytes.toBytes(brand));
				String address = div.getElementsContainingOwnText("地址：").get(0).attr("title");
				insertData(rowDatas, rowKey, DEALER_INFO_FAMILY, "address", Bytes.toBytes(address));
				String latlon = locs.containsKey(id) ? locs.get(id) : "";
				insertData(rowDatas, rowKey, DEALER_INFO_FAMILY, "latlon", Bytes.toBytes(latlon));
			}
			
			if (rowDatas.size() > 0) {
				Table table = HBaseTools.openTable(DEALER_INFO_TABLE);
				if (table != null) {
					HBaseTools.putRowDatas(table, rowDatas);
					HBaseTools.closeTable(table);
				}
			}
		}
	}
	
	@Override
	protected void onContentFetchError(WebURL webUrl) {
		if (proxyConf.isEnable()) {
			String[] params = proxyConf.randomIp().split(":");
			System.getProperties().setProperty("proxySet", "true");
			System.getProperties().setProperty("http.proxyHost", params[0]);
			System.getProperties().setProperty("http.proxyPort", params[1]);
		}
	}
	
}
