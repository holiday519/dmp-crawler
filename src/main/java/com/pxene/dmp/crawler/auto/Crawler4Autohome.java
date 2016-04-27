package com.pxene.dmp.crawler.auto;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Autohome extends BaseCrawler {

	private Log logger = LogFactory.getLog(Crawler4Autohome.class);
	// hbase参数
	private static final String TABLE_NAME = "t_auto_autocode";

	private static final String FAMILY_NAME = "info";

	private static final String ROWKEY_PREFIX = "00030005_";

	// 正则
	private final static String FILTER_REGEX = ".*(\\.(css|js|bmp|gif|jpe?g"
			+ "|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|pdf|rm|smil|wmv|swf|wma|zip|rar|gz))$";
	private class AUTO_CODE_REGEXS {
		public static final String SALE_AUTO = "^http://www\\.autohome\\.com\\.cn/[0-9]*/.*$";
		public static final String UNSALE_AUTO = "^http://www\\.autohome\\.com\\.cn/[0-9]*/sale.html$";
		public static final String STYLE_LIST = "^http://car\\.autohome\\.com\\.cn/config/series/[0-9\\-]*\\.html$";
	}
	// json解析
	private static JsonParser parser = new JsonParser();
	
//	private class USER_INFO_REGEXS {
//
//	}

	// private static final String[] AUTO_CODE_REGEXS = {
	// "^http://www\\.autohome\\.com\\.cn/[0-9]*/.*$",
	// // "^http://www\\.autohome\\.com\\.cn/spec/[0-9]*/$",
	// "^http://car\\.autohome\\.com\\.cn/config/series/[0-9\\-]*\\.html$",
	// "^http://www\\.autohome\\.com\\.cn/[0-9]*/sale.html$"
	// };
	// private static final String[] USER_INFO_REGEXS = {
	// "^http://club\\.autohome\\.com\\.cn/bbs/forum-(a|c|o)-[0-9]*-1\\.html$",
	// "^http://club\\.autohome\\.com\\.cn/bbs/brand-[0-9]*-c-[0-9]*-1.html$",
	// "^http://zhidao\\.autohome\\.com\\.cn/list/s1-[0-9]*.html$",
	// "^http://club\\.autohome\\.com\\.cn/bbs/(thread|threadqa)-(a|c|o)-[0-9]*-[0-9]*-1.html$"
	// };

	@Override
	public boolean shouldVisit(Page referringPage, WebURL webURL) {
		String url = webURL.getURL().toLowerCase();
		if (url.matches(FILTER_REGEX)) {
			return false;
		}
		// autocode
		if (url.matches(AUTO_CODE_REGEXS.SALE_AUTO)
				|| url.matches(AUTO_CODE_REGEXS.UNSALE_AUTO)
				|| url.matches(AUTO_CODE_REGEXS.STYLE_LIST)) {
			return true;
		}
		
		return false;
	}

	@Override
	public void visit(Page page) {
		getAutoCode(page);
		getUserInfo(page);
	}

	private void getAutoCode(Page page) {
		if (page.getWebURL().getURL().matches(AUTO_CODE_REGEXS.STYLE_LIST)) {
			Document doc = Jsoup.parse(((HtmlParseData) page.getParseData()).getHtml());
			if (doc == null) {
				return;
			}
			Elements scripts = doc.select("script");
			for (Element script : scripts) {
				String content = script.html();
				if (content.contains("var config =")) {
					String jStr = StringUtils.regexpExtract(content, "var config = (\\{.*\\});?").trim();
					if (jStr.length() != 0) {
						Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
						// 解析json
						JsonObject jObj = parser.parse(jStr).getAsJsonObject();
						JsonObject result = jObj.get("result").getAsJsonObject();
						// 
						String autoId = result.get("seriesid").getAsString();
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
									insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "style", Bytes.toBytes(style));
								}
								break;
							case "厂商指导价(元)":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String price = StringUtils.regexpExtract(val.getAsJsonObject().get("value").getAsString(), "([.\\d]*)万");
									if (price.matches("[.\\d]+")) {
										insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "price", Bytes.toBytes(Float.parseFloat(price)));
									} else {
										insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "price", Bytes.toBytes(0f));
									}
								}
								break;
							case "级别":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String level = val.getAsJsonObject().get("value").getAsString();
									insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "level", Bytes.toBytes(level));
								}
								break;	
							case "发动机":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String engine = val.getAsJsonObject().get("value").getAsString();
									insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "engine", Bytes.toBytes(engine));
								}
								break;
							case "变速箱":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String gearbox = val.getAsJsonObject().get("value").getAsString();
									insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "gearbox", Bytes.toBytes(gearbox));
								}
								break;
							case "长*宽*高(mm)":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String size = val.getAsJsonObject().get("value").getAsString();
									insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "size", Bytes.toBytes(size));
								}
								break;
							case "车身结构":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String struct = val.getAsJsonObject().get("value").getAsString();
									insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "struct", Bytes.toBytes(struct));
								}
								break;
							case "最高车速(km/h)":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String speed = val.getAsJsonObject().get("value").getAsString();
									if (speed.matches("[\\d]+")) {
										insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "speed", Bytes.toBytes(Integer.parseInt(speed)));
									} else {
										insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "speed", Bytes.toBytes(0));
									}
								}
								break;
							case "工信部综合油耗(L/100km)":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String fuel = val.getAsJsonObject().get("value").getAsString();
									if (fuel.matches("[.\\d]+")) {
										insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "fuel", Bytes.toBytes(Float.parseFloat(fuel)));
									} else {
										insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "fuel", Bytes.toBytes(0f));
									}
								}
								break;
							case "整车质保":
								for (JsonElement val : paramVals) {
									String styleId = val.getAsJsonObject().get("specid").getAsString();
									String pqa = val.getAsJsonObject().get("value").getAsString();
									insertData(rowDatas, ROWKEY_PREFIX + autoId + "_" + styleId, FAMILY_NAME, "pqa", Bytes.toBytes(pqa));
								}
								break;
							default:
								break;
							}
						}
						// 抓取车名
						String name = doc.select("div.subnav-title-name a").first().text();
						for (Map.Entry<String, Map<String, Map<String, byte[]>>> rowData : rowDatas.entrySet()) {
							insertData(rowDatas, rowData.getKey(), FAMILY_NAME, "name", Bytes.toBytes(name));
						}
						
						HTableInterface table = HBaseTools.openTable(TABLE_NAME);
						if (table != null) {
							HBaseTools.putRowDatas(table, rowDatas);
							HBaseTools.closeTable(table);
						}
					}
				}
			}
		}
	}
	
	private void getUserInfo(Page page) {
		
	}
	
}
