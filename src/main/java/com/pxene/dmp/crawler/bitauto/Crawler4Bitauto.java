package com.pxene.dmp.crawler.bitauto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.main.CrawlerManager;
import com.sun.tools.doclets.internal.toolkit.resources.doclets;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Bitauto extends WebCrawler {
	private static final String USERAGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	private Log logger = LogFactory.getLog(Crawler4Bitauto.class);
	
	//入库所需参数
	private static final String ROWKEY_PREFIX="00030007_";
	private static final String TABLE_NAME = "t_auto_autoinfo2";
	private static final String FAMILY_NAME = "auto_info";
	
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" + "|png|tiff?|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	
	private static final String hmcRegex4Car="^http://beijing.huimaiche.com/\\?tracker_u=53_ycsydh$|"
			+ "^http://beijing.huimaiche.com/search/[\\d]*-0-0-0-0-0-0-0-0-0-0/$|"
			+ "^http://beijing.huimaiche.com/[a-zA-Z1-9]*/\\?page=search_model$|"
			+ "^http://beijing.huimaiche.com/[a-zA-Z1-9]*/[\\d]*/$"; 
	
	
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		return  !FILTERS.matcher(href).matches() && href.matches(hmcRegex4Car);
	}
	
	@Override
	public void visit(Page page) {
		String url=page.getWebURL().getURL();
		visitSpecPage(url);
	}

	private void visitSpecPage(String url) {
		if(url.matches("^http://beijing.huimaiche.com/search/$")){
			try {
				Document doc = Jsoup.connect(url).userAgent(USERAGENT).get();
				Elements as = doc.select("div#brandListPanel a");
				String searchBase="http://beijing.huimaiche.com/search/**-0-0-0-0-0-0-0-0-0-0/";
				for (Element a : as) {
//				System.out.println(a.text()+":::"+a.attr("data-code"));
//				System.out.println(searchBase.replace("**", a.attr("data-code")));
					CrawlerManager.controller.addSeed(searchBase.replace("**", a.attr("data-code")));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(url.matches("^http://beijing.huimaiche.com/[a-zA-Z1-9]*/\\?page=search_model$")){
			try {
				Document doc = Jsoup.connect(url).userAgent(USERAGENT).get();
				Elements lis = doc.select("ul.car-type-ul li");
//				System.out.println(lis.size());
				for (Element li : lis) {
//					System.out.println(li.attr("data-url"));
					visitSpecPage(li.attr("data-url"));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(url.matches("^http://beijing.huimaiche.com/[a-zA-Z1-9]*/([\\d]{5,})/$")){
			org.mortbay.log.Log.info("**URL***"+url);
			try {
				Document doc = Jsoup.connect(url).userAgent(USERAGENT).get();
				String brandName=doc.select("a#a_allserial").text();
//				System.out.println("brandName  "+brandName);
				String specName=doc.select("a#a_allserial").first().nextElementSibling().text();
//				System.out.println("specName  "+specName);
				String peizhiName=doc.select("a#a_allserial").first().nextElementSibling().nextElementSibling().text();
//				System.out.println("specPeizhiName  "+peizhiName);
				String brandId=StringUtils.regexpExtract(doc.select("script").toString(), "brandId: (.*?),");
//				System.out.println("branId  "+brandId);
				String carId=StringUtils.regexpExtract(url, "^http://beijing.huimaiche.com/[a-zA-Z1-9]*/([\\d]*)/$");
//				System.out.println("carId  "+carId);
				String priceStr=doc.select("del.guide-price-value").text();
				Float price=Float.parseFloat(StringUtils.regexpExtract(priceStr, "(.*?)万"));
//				System.out.println("price  "+price);
				
				//入库
				Map<String, byte[]> datas = new HashMap<String, byte[]>();
				datas.put("auto_name", Bytes.toBytes(brandName+"#"+specName+"#"+peizhiName));
				datas.put("manu_price", Bytes.toBytes(price));
				
				HTableInterface table = HBaseTools.openTable(TABLE_NAME);
				if (table != null) {
					String rowKey = ROWKEY_PREFIX + brandId + "_" + carId;
//					HBaseTools.putColumnDatas(table, rowKey, FAMILY_NAME, datas);
					HBaseTools.closeTable(table);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
