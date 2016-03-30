package com.pxene.dmp;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class Controller {
	private static final String UA1="Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36";
	
	public static void main(String[] args) throws Exception {
//		if("getcarinofo".equals(args[0]))
		getCarInfo();
	}

	private static void getCarInfo() throws Exception {
		String crawlStorageFolder1 = "temp1";
		String crawlStorageFolder2 = "temp2";
		String crawlStorageFolder3 = "temp3";
		// 抓取线程数
		int numberOfCrawlers = 40;

		CrawlConfig config1 = new CrawlConfig();
		CrawlConfig config2 = new CrawlConfig();
		CrawlConfig config3 = new CrawlConfig();
		// 临时存储路径
		config1.setCrawlStorageFolder(crawlStorageFolder1);
		config2.setCrawlStorageFolder(crawlStorageFolder2);
		config3.setCrawlStorageFolder(crawlStorageFolder3);

//		 config.setResumableCrawling(true);

		// 抓取深度
//		 config.setMaxDepthOfCrawling(4);
		
		//设置UA
		config1.setUserAgentString(UA1);
		config2.setUserAgentString(UA1);
		config3.setUserAgentString(UA1);
		
		//webFetch設置
		PageFetcher pageFetcher1 = new PageFetcher(config1);
		PageFetcher pageFetcher2 = new PageFetcher(config2);
		PageFetcher pageFetcher3 = new PageFetcher(config3);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher1);
		
		//controller設置
		CrawlController controller1 = new CrawlController(config1, pageFetcher1, robotstxtServer);
		CrawlController controller2 = new CrawlController(config2, pageFetcher2, robotstxtServer);
		CrawlController controller3 = new CrawlController(config3, pageFetcher3, robotstxtServer);
		String[] crawler1Domains = {"http://www.autohome.com.cn/beijing/"};
		String[] crawler2Domains = {"http://beijing.bitauto.com/"};
		String[] crawler3Domains = {"http://beijing.huimaiche.com/"};
		controller1.setCustomData(crawler1Domains);
		controller2.setCustomData(crawler2Domains);
		controller3.setCustomData(crawler3Domains);
		
		// 针对汽车之家
		controller1.addSeed("http://www.autohome.com.cn/beijing/");

		// 针对易车
		controller2.addSeed("http://beijing.bitauto.com/");
		
		// 针对惠买车
		controller3.addSeed("http://beijing.huimaiche.com/");
		
		controller1.startNonBlocking(MyCrawler4auto.class, numberOfCrawlers);
		controller1.waitUntilFinish();
		
		controller2.startNonBlocking(MyCrawler4yiche.class, numberOfCrawlers);
		controller2.waitUntilFinish();
		
		controller3.startNonBlocking(MyCrawler4hmc.class, numberOfCrawlers);
		controller3.waitUntilFinish();
	}
}
