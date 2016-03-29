package com.pxene.dmp;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class Controller {
	public static void main(String[] args) throws Exception {
		String crawlStorageFolder = "temp";
		// 抓取线程数
		int numberOfCrawlers = 50;

		CrawlConfig config = new CrawlConfig();
		// 临时存储路径
		config.setCrawlStorageFolder(crawlStorageFolder);
		
//		config.setResumableCrawling(true);
		
		
		// 抓取深度
//		config.setMaxDepthOfCrawling(4);

		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig,
				pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher,
				robotstxtServer);


		//针对汽车之家
			controller.addSeed("http://www.autohome.com.cn/beijing/");
		
		//针对易车
			controller.addSeed("http://beijing.bitauto.com/");
			
		
		//针对惠买车
			controller.addSeed("http://beijing.huimaiche.com/");
			controller.start(MyCrawler.class, numberOfCrawlers);
	}
}
