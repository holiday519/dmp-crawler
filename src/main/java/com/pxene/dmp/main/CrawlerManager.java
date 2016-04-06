package com.pxene.dmp.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.pxene.dmp.common.ProxyTool;
import com.pxene.dmp.common.SeedConfig;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.CrawlController.WebCrawlerFactory;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.crawler.authentication.AuthInfo;
import edu.uci.ics.crawler4j.crawler.authentication.BasicAuthInfo;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CrawlerManager {
	private static final String USERAGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
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
			// 抓取深度
			//config.setMaxDepthOfCrawling(1);
			// 登陆
			List<AuthInfo> infos = new ArrayList<AuthInfo>();
			AuthInfo info = new BasicAuthInfo("holiday519", "history422", "http://account.autohome.com.cn/");
			infos.add(info);
			config.setAuthInfos(infos);
			//設置代理
//			Map<String, String> ipInfo = ProxyTool.getIpInfo();
//			config.setProxyHost(ipInfo.get("ip"));
//			config.setProxyHost(ipInfo.get("port"));
			
			config.setCrawlStorageFolder(crawlStorageFolder);
			config.setUserAgentString(USERAGENT);

			PageFetcher pageFetcher = new PageFetcher(config);
			RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
			// 关闭robot协议
			robotstxtConfig.setEnabled(false);
			robotstxtConfig.setUserAgentName(USERAGENT);
			RobotstxtServer robotstxtServer = new RobotstxtServer(
					robotstxtConfig, pageFetcher);
			CrawlController controller = new CrawlController(config,
					pageFetcher, robotstxtServer);

			for (String seed : SeedConfig.getSeeds(className)) {
				controller.addSeed(seed);
			}

			controller.start(new WebCrawlerFactory<WebCrawler>() {
				@Override
				public WebCrawler newInstance() throws Exception {
					return (WebCrawler) Class.forName(
							packageName + "." + className).newInstance();
				}
			}, numberOfCrawlers);
		} else {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("options", options);
		}

	}

	
}
