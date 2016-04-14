package com.pxene.dmp.crawler.autocode;

import java.io.UnsupportedEncodingException;
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
	
	private static final String bitauto4Car="^http://car.bitauto.com/$|"
			+ "^http://car.bitauto.com/tree_chexing/mb_[\\d]*/$|"
			+ "^http://car.bitauto.com/[a-zA-Z|-]*/$|"
			+ "^http://car.bitauto.com/[a-zA-Z|-]*/peizhi/$|"
			+ "^http://car.bitauto.com/[a-zA-Z|-]*/m[\\d]*/$";
	
	private static  String treeUrlBase="http://car.bitauto.com/tree_chexing/mb_**/";
	
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		return  !FILTERS.matcher(href).matches() && href.matches(bitauto4Car);
	}
	
	@Override
	public void visit(Page page) {
		String url=page.getWebURL().getURL();
//		System.out.println(url);
		visitSpecPage(url);
		
	}

	private void visitSpecPage( String url) {
		if(url.matches("^http://car.bitauto.com/$")){
			for(int i=2;i<254;i++){
				CrawlerManager.controller.addSeed(treeUrlBase.replace("**", i+""));
			}
		}
		
		if(url.matches("^http://car.bitauto.com/tree_chexing/mb_[\\d]*/$")){
			try {
				Document doc = Jsoup.connect(url).userAgent(USERAGENT).get();
				Elements brandUrls = doc.select("a[stattype=car]");
//				System.out.println(brandUrls.size());
				for (Element brandUrl : brandUrls) {
					String brand=brandUrl.absUrl("href");
					CrawlerManager.controller.addSeed(brand);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		if(url.matches("^http://car.bitauto.com/[a-zA-Z|-]*/m([\\d]*)/$")){
			logger.info("#####URL######"+url);
			try {
				Document doc = Jsoup.connect(url).userAgent(USERAGENT).get();
				String priceInfo=StringUtils.regexpExtract(doc.select("div#jiaGeDetail>span.s1>em").text(), "([\\d]*)万");
				Float manu_price=new Float(0);
				if(priceInfo.length()>0){
					manu_price=Float.parseFloat(priceInfo);
				}
//				System.out.println("manu_price:::"+manu_price);
				 Elements refers = doc.select("ul.ul-set span");
				 String com_wear="";
				if(refers.size()-1>0){
					com_wear = refers.get(0).text();
				}
				
//				System.out.println("com_wear:::"+com_wear);
				String gearbox="";
				if(refers.size()-1>1){
					gearbox = refers.get(1).text();
				}
//				System.out.println("gearbox:::"+gearbox);
				String cc="";
				if(refers.size()-1>2){
					cc = refers.get(0).text();
				}
//				System.out.println("cc:::"+cc);
				String engine="";
				if(refers.size()-1>3){
					engine = refers.get(3).text();
				}
//				System.out.println("engine:::"+engine);
				 Elements pqaInfo = doc.select("div[class=car_config car_top_set] table tbody td[class=td-b-sty td-p-sty]");
				 String pqa="";
				if(pqaInfo.size()>0){
					pqa= pqaInfo.first().text();
				}
				
//				System.out.println("qpa:::"+pqa);
				
				String brandId=doc.select("div.bt_ad ins").first().attr("adplay_brandid");
//				System.out.println("brandId:::"+brandId);
				String carId=StringUtils.regexpExtract(url, "m([\\d]*)/$");
//				System.out.println("carId:::"+carId);
				Elements nameEle = doc.select("div.car_navigate a");
				StringBuffer nameBuffer=new StringBuffer();
				for(int i=0;i<nameEle.size();i++){
					if(i<2)
						continue;
					nameBuffer.append(nameEle.get(i).text()).append("#");
				}
				String specName=doc.select("div.car_navigate strong").text();
				String name=nameBuffer.substring(0, nameBuffer.length()-1)+"#"+specName;
//				System.out.println("name:::"+name);
				
				//入库
				Map<String, byte[]> datas = new HashMap<String, byte[]>();
				datas.put("auto_name", Bytes.toBytes(name));
				datas.put("manu_price", Bytes.toBytes(manu_price));
				datas.put("com_wear", Bytes.toBytes(com_wear));
				datas.put("pqa", Bytes.toBytes(pqa));
				datas.put("engine", Bytes.toBytes(engine));
				datas.put("gearbox", Bytes.toBytes(gearbox));
				datas.put("cc", Bytes.toBytes(cc)); //排量
				
				HTableInterface table = HBaseTools.openTable(TABLE_NAME);
				if (table != null) {
					String rowKey = ROWKEY_PREFIX + brandId + "_" + carId;
					HBaseTools.putColumnDatas(table, rowKey, FAMILY_NAME, datas);
					HBaseTools.closeTable(table);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}

	
}
