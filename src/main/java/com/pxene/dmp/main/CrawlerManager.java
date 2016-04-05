package com.pxene.dmp.main;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.pxene.dmp.common.SeedConfig;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.CrawlController.WebCrawlerFactory;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CrawlerManager {
	private static final String USERAGENT="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	public static void main(String[] args) throws Exception {
		// 基本配置
		final String packageName = "com.pxene.dmp.crawler";
		String crawlStorageFolder = "temp";
		int numberOfCrawlers = 40;
		// 命令行配置
		Options options = new Options();
		options.addOption("className", true, "input class name of crawler");
		CommandLineParser parser = new BasicParser();
		CommandLine line = parser.parse(options, args);
		
		if (line.hasOption("className")) {
			final String className = line.getOptionValue("className");
			CrawlConfig config = new CrawlConfig();
	        config.setCrawlStorageFolder(crawlStorageFolder);
	        config.setUserAgentString(USERAGENT);
	        
	        PageFetcher pageFetcher = new PageFetcher(config);
	        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
	        robotstxtConfig.setEnabled(false); //关闭robot协议
	        robotstxtConfig.setUserAgentName(USERAGENT);
	        robotstxtConfig.setUserAgentName("");
	        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
	        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
	        
	        for (String seed : SeedConfig.getSeeds(className)) {
	        	controller.addSeed(seed);
	        }
	        
	        controller.start(new WebCrawlerFactory<WebCrawler>() {
				@Override
				public WebCrawler newInstance() throws Exception {
					return (WebCrawler)Class.forName(packageName + "." + className).newInstance();
				}
			}, numberOfCrawlers);
		} else {
			HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("options", options);
		}
		
	}
	
}
