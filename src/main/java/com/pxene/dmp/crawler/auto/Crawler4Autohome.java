package com.pxene.dmp.crawler.auto;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.service.CookieSigner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pxene.dmp.common.AjaxClient;
import com.pxene.dmp.common.CookieTools;
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
		public static final String FORUM_LIST = "^http://club\\.autohome\\.com\\.cn/bbs/forum-(a|c|o)-[0-9]+-[0-9]+\\.html.*$";
		public static final String BRAND_LIST = "^http://club\\.autohome\\.com\\.cn/bbs/brand-[0-9]+-c-[0-9]+-[0-9]+\\.html.*$";
		public static final String ZHIDAO_LIST = "^http://zhidao\\.autohome\\.com\\.cn/list/s1-[0-9]+\\.html$";
		public static final String POST_DETAIL = "^http://club\\.autohome\\.com\\.cn/bbs/(thread|threadqa)-(a|c|o)-[0-9]+-[0-9]+-[0-9]+\\.html$";
	}
	private class USER_INFO_REGEXS {
		public static final String WILD_CARD = "^http://i\\.autohome\\.com\\.cn/[0-9]+.*$";
		public static final String HOME_PAGE = "^http://i\\.autohome\\.com\\.cn/[0-9]+$";
		public static final String BASE_INFO = "^http://i\\.autohome\\.com\\.cn/[0-9]+/info$";
		public static final String FOLLOWING = "^http://i\\.autohome\\.com\\.cn/[0-9]+/following(?!\\?page).*$";
		public static final String FOLLOWERS = "^http://i\\.autohome\\.com\\.cn/[0-9]+/followers(?!\\?page).*$";
		public static final String USER_ID = "i\\.autohome\\.com\\.cn/([0-9]+)/";
		
	}
	private class DEALER_INFO_REGEXS {
		
	}
	
	private class AJAX_URL_TMPL {
		public static final String USER_INFO = "http://i.autohome.com.cn/ajax/home/GetUserInfo?userid={userid}&r={random}&_={timestamp}";
		public static final String USER_INFO_REFERER = "http://i.autohome.com.cn/{userid}";
		public static final String USER_FOLLOWING_PAGE = "http://i.autohome.com.cn/{userid}/following?page={pageno}";
		public static final String USER_FOLLOWERS_PAGE = "http://i.autohome.com.cn/{userid}/followers?page={pageno}";
	}
	
	// json解析
	private static JsonParser parser = new JsonParser();

	public Crawler4Autohome() {
		super("/" + Crawler4Autohome.class.getName().replace(".", "/") + ".json");
		// 将cookie存入静态变量
		String url = loginConf.getCookie().getUrl();
		String usrkey = loginConf.getCookie().getUsekey();
		String username = loginConf.getUsername();
		String pwdkey = loginConf.getCookie().getPwdkey();
		String password = DigestUtils.md5Hex(loginConf.getPassword());
		cookie = CookieTools.get(url, usrkey, username, pwdkey, password);
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
		if (url.matches(POST_INFO_REGEXS.FORUM_LIST)
				|| url.matches(POST_INFO_REGEXS.BRAND_LIST)
				|| url.matches(POST_INFO_REGEXS.ZHIDAO_LIST)
				|| url.matches(POST_INFO_REGEXS.POST_DETAIL)
				|| url.matches(USER_INFO_REGEXS.WILD_CARD)) {
			return true;
		}
		
		return false;
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		if (url.matches(AUTO_INFO_REGEXS.STYLE_LIST)) {
			getAutoInfo(url, ((HtmlParseData) page.getParseData()).getHtml());
		}
		if (url.matches(USER_INFO_REGEXS.WILD_CARD)) {
			getUserInfo(url, ((HtmlParseData) page.getParseData()).getHtml());
		}
		if (url.matches(POST_INFO_REGEXS.POST_DETAIL)) {
			getPostInfo(url, ((HtmlParseData) page.getParseData()).getHtml());
		}
	}

	private void getAutoInfo(String url, String html) {
		Document doc = parseHtml(html);
		if (doc == null) {
			return;
		}
		Elements scripts = doc.select("script");
		for (Element script : scripts) {
			String content = script.html();
			if (content.contains("var config =")) {
				String jStr = StringUtils.regexpExtract(content, "var config = (\\{.*?\\});").trim();
				if (jStr.length() != 0) {
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
								String price = StringUtils.regexpExtract(val.getAsJsonObject().get("value").getAsString(), "([.\\d]*)万");
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
		// 获取userId
		Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
		String userId = StringUtils.regexpExtract(url, USER_INFO_REGEXS.USER_ID);
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
			int vip = 1;
			Element elem = doc.select("i.icon16-vorange").get(0);
			if (elem.attr("style").contains("display:none")) {
				vip = 0;
			}
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "vip", Bytes.toBytes(vip));
		}
		// 基本资料
		if (url.matches(USER_INFO_REGEXS.BASE_INFO)) {
			Element baseInfo = doc.select("#divuserinfo").get(0);
			String name = baseInfo.child(0).ownText();
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "name", Bytes.toBytes(name));
			String sex = baseInfo.child(1).ownText().contains("男") ? "0" : "1";
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "sex", Bytes.toBytes(sex));
			String city = baseInfo.child(3).ownText();
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "city", Bytes.toBytes(city));
		}
		// 他的关注
		if (url.matches(USER_INFO_REGEXS.FOLLOWING)) {
			Set<String> codes = new HashSet<String>();
			Element dynamic = doc.select("#dynamic .subdyn2").get(0);
			int amount = Integer.parseInt(StringUtils.regexpExtract(dynamic.text(), "关注([\\d]+)人"));
			int pageSize = amount / 20 + 1;
			for (int pageNo=1; pageNo<=pageSize; pageNo++) {
				String pageUrl = AJAX_URL_TMPL.USER_FOLLOWING_PAGE
						.replace("{userid}", userId)
						.replace("{pageno}", String.valueOf(pageNo));
				Document pageDoc = connectUrl(pageUrl, cookie);
				Elements elems = pageDoc.select("#ulList li");
				for (Element elem : elems) {
					codes.add(elem.attr("id"));
				}
			}
			String following = org.apache.commons.lang.StringUtils.join(codes, ",");
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "following", Bytes.toBytes(following));
		}
		// 他的粉丝
		if (url.matches(USER_INFO_REGEXS.FOLLOWERS)) {
			Set<String> codes = new HashSet<String>();
			Element dynamic = doc.select("#dynamic .subdyn2").get(0);
			int amount = Integer.parseInt(StringUtils.regexpExtract(dynamic.text(), "已有([\\d]+)人"));
			int pageSize = amount / 20 + 1;
			for (int pageNo=1; pageNo<=pageSize; pageNo++) {
				String pageUrl = AJAX_URL_TMPL.USER_FOLLOWERS_PAGE
						.replace("{userid}", userId)
						.replace("{pageno}", String.valueOf(pageNo));
				Document pageDoc = connectUrl(pageUrl, cookie);
				Elements elems = pageDoc.select("#ulList li");
				for (Element elem : elems) {
					codes.add(elem.attr("id"));
				}
			}
			String followers = org.apache.commons.lang.StringUtils.join(codes, ",");
			insertData(rowDatas, rowKey, USER_INFO_FAMILY, "followers", Bytes.toBytes(followers));
		}
		// 
		
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
	
	public static void main(String[] args) throws IOException {
//		Document doc = Jsoup.connect("http://i.autohome.com.cn/5276849").get();
//		Element elem = doc.select("#userGrade").get(0);
//		System.out.println(elem.text());
		
//		CloseableHttpClient client = HttpClients.createDefault();
//		String url = "http://i.autohome.com.cn/ajax/home/GetUserInfo?userid=8599645&r=0.9891249693446999&_=" + (new Date()).getTime();
//		System.out.println(url);
//		HttpGet request = new HttpGet(url);
//		//request.setHeader("Accept", "application/json, text/javascript, */*; q=0.01");
//		//request.setHeader("Accept-Encoding", "gzip, deflate, sdch");
//		//request.setHeader("Accept-Language", "zh-CN,zh;q=0.8,en;q=0.6");
//		//request.setHeader("Cache-Control", "max-age=0");
//		//request.setHeader("Connection", "keep-alive");
//		//request.setHeader("Cookie", "cookieCityId=110100; sessionid=33513229-A778-D029-947E-ADD990924E7C%7C%7C2016-05-10+15%3A36%3A53.686%7C%7Cwww.baidu.com; sessionuid=33513229-A778-D029-947E-ADD990924E7C||2016-05-10+15%3A36%3A53.686||www.baidu.com; mylead_26656633=11; __utma=1.851407740.1463065374.1463065374.1463561250.2; __utmz=1.1463065374.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; sessionip=36.110.73.210; wwwjbtab=0%2C0; AccurateDirectseque=404; historybbsName4=c-2963%7C%E5%AE%9D%E9%A9%AC3%E7%B3%BBGT%7C%2Cc-3941%7C%E5%AE%9D%E9%A9%AC2%E7%B3%BB%E6%97%85%E8%A1%8C%E8%BD%A6%7C%2Cc-3667%7CAC%20Schnitzer%20X4%7C%2Cc-3545%7C%E7%91%9E%E9%A3%8ES2%7C%2Cc-266%7C%E9%98%BF%E6%96%AF%E9%A1%BF%26%23183%3B%E9%A9%AC%E4%B8%81%7C%2Cc-155%7C%E4%B9%90%E9%A9%B0%7C%2F2012%2F12%2F13%2F72c2a89d-5a87-4ea2-8fe6-5a98ab0defab_s.jpg%2Cc-3774%7C%E5%AE%9D%E9%AA%8F330%7C%2Cc-692%7C%E5%A5%A5%E8%BF%AAA4L%7C%2F2014%2F11%2F26%2F9efc86ba-1281-496f-ba2b-77039acda441_s.jpg%2Ca-100030%7C%E6%B5%99%E6%B1%9F%7C%2F2012%2F10%2F16%2Fd0b32eda-0dd1-4c29-8038-5306f95c3c96_s.jpg%2Ca-100024%7C%E4%B8%8A%E6%B5%B7%7C%2F2012%2F10%2F17%2Fde8dfe68-8445-423b-8041-1e97eae285fd_s.jpg; ASP.NET_SessionId=uslt5fv4ttykrzq33pxb0ha1; sessionfid=3368193440; area=339999; ref=www.baidu.com%7C%7C0%7C8-1%7C2016-05-19+10%3A27%3A44.029%7C2016-05-10+15%3A36%3A53.686");
//		//request.setHeader("Host", "i.autohome.com.cn");
//		request.setHeader("Referer", "http://i.autohome.com.cn/8599645");
//		//request.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36");
//		//request.setHeader("X-Requested-With", "XMLHttpRequest");
//		
//		HttpResponse resp = client.execute(request);
//		InputStream is = resp.getEntity().getContent();
//		byte[] b = new byte[1000];
//		is.read(b);
//		System.out.println(new String(b, "GBK"));
		
//		System.out.println(resp.getStatusLine());
//		System.out.println((new Date()).getTime());
		
		//System.out.println(StringUtils.regexpExtract("http://i.autohome.com.cn/5880335/club/topic?pvareaid=104339", USER_INFO_REGEXS.USER_ID));
		
//		Map<String, String> params = new HashMap<String, String>();
//		params.put("userid", "5880335");
//		params.put("r", "0.9891249693446999");
//		params.put("_", (new Date()).getTime() + "");
//		params.put("Referer", "http://i.autohome.com.cn/8599645");
//		AjaxClient client = new AjaxClient.Builder("http://i.autohome.com.cn/ajax/home/GetUserInfo?userid=8599645&r=0.9891249693446999&_=" + (new Date()).getTime(), params).build();
//		System.out.println(client.execute().toString());
//		JsonObject json = client.execute();
//		System.out.println(json.get("UserGrade").getAsJsonObject().get("Grade").getAsString());
		
//		Document doc = Jsoup.connect("http://i.autohome.com.cn/15415544").get();
//		Element elem = doc.select("i.icon16-vorange").get(0);
//		System.out.println(elem.attr("style"));
		
//		Document doc = Jsoup.connect("http://i.autohome.com.cn/20869435/following?page=2")
//				.cookie("cookieCityId", "110100")
//				.cookie("sessionid", "33513229-A778-D029-947E-ADD990924E7C%7C%7C2016-05-10+15%3A36%3A53.686%7C%7Cwww.baidu.com")
//				.cookie("sessionuid", "33513229-A778-D029-947E-ADD990924E7C||2016-05-10+15%3A36%3A53.686||www.baidu.com")
//				.cookie("mylead_26656633", "11")
//				.cookie("__utma", "1.851407740.1463065374.1463065374.1463561250.2")
//				.cookie("__utmz", "1.1463065374.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic")
//				.cookie("sessionip", "36.110.73.210")
//				.cookie("wwwjbtab", "0%2C0")
//				.cookie("AccurateDirectseque", "404")
//				.cookie("historybbsName4", "c-3667%7CAC%20Schnitzer%20X4%7C%2Cc-3545%7C%E7%91%9E%E9%A3%8ES2%7C%2Cc-266%7C%E9%98%BF%E6%96%AF%E9%A1%BF%26%23183%3B%E9%A9%AC%E4%B8%81%7C%2Cc-155%7C%E4%B9%90%E9%A9%B0%7C%2F2012%2F12%2F13%2F72c2a89d-5a87-4ea2-8fe6-5a98ab0defab_s.jpg%2Cc-3774%7C%E5%AE%9D%E9%AA%8F330%7C%2Cc-692%7C%E5%A5%A5%E8%BF%AAA4L%7C%2F2014%2F11%2F26%2F9efc86ba-1281-496f-ba2b-77039acda441_s.jpg%2Ca-100030%7C%E6%B5%99%E6%B1%9F%7C%2F2012%2F10%2F16%2Fd0b32eda-0dd1-4c29-8038-5306f95c3c96_s.jpg%2Ca-100024%7C%E4%B8%8A%E6%B5%B7%7C%2F2012%2F10%2F17%2Fde8dfe68-8445-423b-8041-1e97eae285fd_s.jpg%2Cc-3412%7C%E5%AE%9D%E9%AA%8F730%7C%2Cc-66%7C%E5%AE%9D%E9%A9%AC3%E7%B3%BB%7C%2F2012%2F12%2F3%2F75e617a9-58bc-4c85-96f9-184e6496e1c0_s.jpg")
//				.cookie("sessionfid", "1107460084")
//				.cookie("ASP.NET_SessionId", "ikkmbkprn5ket4qsg1ftvk3q")
//				.cookie("pcpopclub", "BB106DA07D40F218049A61649D449DFBE583C911D3144A54A6F9361259CA1B6A62BC58B93426868FFD0306A7A09211B6EB5F409B9B8B30D05930C055D348947722281DC47DB0BD61257228C47664EDA9D6F71870E9C8B1110C2076E17A0C986D9E6BA11AC58618D2FC3EC2D10384E80A7372987FC742450E58FD7E0FBCF15FD56435BBA11DAF736A05FB786F512FE2C906C86709B2884EE40B4DFD47CA710D6A4D39CFAB819352166494EC58368AFD8FFFFB41F5B3C25E091693916CE06942CE38421431F20A37FC9B8110A55BA6D78AB15E10CA5DF7CC7002931F93B7E64FC13496C786E41230CB3327345897E2E6F26B3187BFC7685C39B981A75BB217B904500C62FF84424AA7B49295990CFCA65936414E39904F0957F42E495A221C43F08EA62AD1D8E9E174735E2C7C12FD22289F7CE454")
//				.cookie("clubUserShow", "26656633|3589|2|holiday519|0|0|0||2016-05-20 15:17:38|0")
//				.cookie("autouserid", "26656633")
//				.cookie("ref", "www.baidu.com%7C%7C0%7C8-1%7C2016-05-20+15%3A17%3A41.203%7C2016-05-10+15%3A36%3A53.686")
//				.cookie("sessionvid", "89F6E2DF-A727-90AE-9FA9-6CF746ABD0E0")
//				.cookie("area", "339999")
//				.cookie("sessionuserid", "26656633")
//				.cookie("sessionlogin", "6eeef4d30a5c4eb08d4b8297af54f8f40196bf79").get();
//		System.out.println(doc.select("#dynamic .subdyn2").get(0).text());
		
//		System.out.println("http://i.autohome.com.cn/20869435/following?page=2".matches(USER_INFO_REGEXS.FOLLOWING));
		
//		System.out.println(21 / 20 + 1);
		
//		Map<String, String> co = CookieTools.get("http://account.autohome.com.cn/Login/ValidIndex"
//				, "name", "qczjcjh123"
//				, "pwd", DigestUtils.md5Hex("cjh4321"));
//		System.out.println(co);
//		Document doc = Jsoup.connect("http://i.autohome.com.cn/15415544/following").cookies(co).get();
//		Set<String> codes = new HashSet<String>();
//		Element dynamic = doc.select("#dynamic .subdyn2").get(0);
//		int amount = Integer.parseInt(StringUtils.regexpExtract(dynamic.text(), "关注([\\d]+)人"));
//		int pageSize = amount / 20 + 1;
//		for (int pageNo=1; pageNo<=pageSize; pageNo++) {
//			String pageUrl = AJAX_URL_TMPL.USER_FOLLOWING_PAGE
//					.replace("{userid}", "15415544")
//					.replace("{pageno}", String.valueOf(pageNo));
//			Document pageDoc = Jsoup.connect(pageUrl).cookies(co).get();
//			Elements elems = pageDoc.select("#ulList li");
//			for (Element elem : elems) {
//				codes.add(elem.attr("id"));
//			}
//		}
//		String following = org.apache.commons.lang.StringUtils.join(codes, ",");
//		System.out.println(following);
		
		
	}
	
}
