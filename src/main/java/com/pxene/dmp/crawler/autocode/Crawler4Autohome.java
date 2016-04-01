package com.pxene.dmp.crawler.autocode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Put;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.HBaseDAOImp;
import com.pxene.dmp.common.URLUtils;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Autohome extends WebCrawler {

	private static final String REGEX = "^http://www\\.autohome\\.com\\.cn.*?";
	private static final String SERIES_REGEX = "^http://www.autohome.com.cn/spec/([\\d]*)/$";
	

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		return url.getURL().toLowerCase().matches(REGEX);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
//		System.out.println(url);
		detailpage2hbase(url); //针对商品详情页
	}

	//商品詳情頁入庫
	private void detailpage2hbase(String url) {
		Pattern pattern = Pattern.compile(SERIES_REGEX);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			System.out.println("*URL***" + url);
			try {
				Document doc = Jsoup.connect(url).get();
				Elements bbs = doc.getElementsByAttributeValueMatching("href", "^/[\\d]*/$");
				String brandId = URLUtils.regexpExtract(bbs.get(0).attr("href"), "/([\\d]*)/"); // 品牌id
				String brandName = bbs.get(0).ownText();// 品牌名

				String specId = URLUtils.regexpExtract(doc.select("div.subnav-title-name>a").attr("href"), "/([\\d]*)/");// 具体id

				String specName = doc.select("div.subnav-title-name>a>h1").text(); // 具体名

				Elements pps = doc.getElementsByAttribute("data-price");
				String manu_price = pps.get(0).attr("data-price").split("万")[0];

				Elements coms = doc.select("div.cardetail-infor-car>ul.fn-clear");

				Elements lis = coms.get(0).children();
				String source = URLUtils.regexpExtract(lis.get(0).text(), "：(.*?)分"); // 评分

				String oil_wear = URLUtils.regexpExtract(lis.get(1).text(), "：(.*?)\\("); // 车主油耗

				String size = lis.get(2).text().split("：")[1]; // 车身尺寸
				String com_wear = URLUtils.regexpExtract(lis.get(3).text(), "：(.*?)\\("); // 综合油耗
				String struct = lis.get(4).text().split("：")[1]; // 车身结构
				String pqa = lis.get(5).text().split("：")[1]; // 整车质保
				String engine = lis.get(6).text().split("：")[1]; // 发动机
				String gearbox = lis.get(7).text().split("：")[1]; // 变速箱
				String driver = lis.get(8).text().split("：")[1]; // 驱动方式

				String rowKey = "00030005_" + brandId + "_" + specId;
				Put put = new Put(rowKey.getBytes());
				
				put.add("auto_info".getBytes(), "manu_price".getBytes(), manu_price.getBytes());
				
				put.add("auto_info".getBytes(), "size".getBytes(), size.getBytes());
				
				put.add("auto_info".getBytes(), "driver".getBytes(), driver.getBytes());

				String auto_name = brandName + "#" + specName;
				put.add("auto_info".getBytes(), "auto_name".getBytes(), auto_name.getBytes());

				if (source != null && source.length() > 0)
					put.add("auto_info".getBytes(), "source".getBytes(), source.getBytes());

				if (oil_wear != null && oil_wear.length() > 0)
					put.add("auto_info".getBytes(), "oil_wear".getBytes(), oil_wear.getBytes());

				if (com_wear != null && com_wear.length() > 0)
					put.add("auto_info".getBytes(), "com_wear".getBytes(), com_wear.getBytes());

				put.add("auto_info".getBytes(), "struct".getBytes(), struct.getBytes());

				if (pqa != null && pqa.length() > 0)
					put.add("auto_info".getBytes(), "pqa".getBytes(), pqa.getBytes());

				put.add("auto_info".getBytes(), "engine".getBytes(), engine.getBytes());
				put.add("auto_info".getBytes(), "gearbox".getBytes(), gearbox.getBytes());

				 HBaseDAOImp.save(put, "t_auto_autoinfo"); //入库
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

}
