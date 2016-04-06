package com.pxene.dmp.crawler.autocode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.ProxyTool;
import com.pxene.dmp.common.StringUtils;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Autohome extends WebCrawler {

	private static final String USERAGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	// 网站url（配置全站的url才能将url抓全）
	private static final String SITE_REGEX = "^http://.*?\\.autohome\\.com\\.cn/.*?";
	
	// 提取style信息的url
	private static final String STYLE_REGEX = "^http://www\\.autohome\\.com\\.cn/spec/[\\d]*/$";
	
	private static final String TABLE_NAME = "t_auto_autoinfo";
	
	private static final String FAMILY_NAME = "auto_info";
	
	private static final String ROWKEY_PREFIX = "00030005_";
	
	private Log log = LogFactory.getLog(Crawler4Autohome.class);
	
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().matches(SITE_REGEX);
	}
	
	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		if (url.matches(STYLE_REGEX)) {
			log.info("****"+page.getWebURL().getURL()); //日志打印
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

}
