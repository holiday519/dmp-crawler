package com.pxene.dmp.main;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.pxene.dmp.common.CjhCookieTool;
import com.pxene.dmp.common.ProxyTool;
import com.pxene.dmp.common.SeedConfig;
import com.pxene.dmp.common.UATool;

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
	public static CrawlController controller;
	public static CrawlConfig config;
	public static void main(String[] args) throws Exception {
		
		//更新cookie和ip代理文件---项目跑起来的时候打开，测试时候不建议经常打开
//		CjhCookieTool.updateAllCookies();
//		ProxyTool.updataIps();
		
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
			config = new CrawlConfig();
			// 抓取深度
//			 config.setMaxDepthOfCrawling(1);
			// 汽車之家登陆
			List<AuthInfo> infos = new ArrayList<AuthInfo>();
			AuthInfo info = new BasicAuthInfo("holiday519", "history422", "http://account.autohome.com.cn/");
			AuthInfo bitinfo = new BasicAuthInfo("18611434755", "yccjh12345", "http://i.yiche.com/authenservice/login.html");
			infos.add(info);
			infos.add(bitinfo);
			config.setAuthInfos(infos);
			
			// 代理设置
			String ipstr = ProxyTool.getIpInfo();
			config.setProxyHost(ipstr.split(":")[0]);
			config.setProxyPort(Integer.parseInt(ipstr.split(":")[1]));
		

			config.setCrawlStorageFolder(crawlStorageFolder);
			config.setUserAgentString(UATool.getUA());
			config.setSocketTimeout(5000);
			config.setConnectionTimeout(5000);

			PageFetcher pageFetcher = new PageFetcher(config);
			RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
			// 关闭robot协议
			robotstxtConfig.setEnabled(false);
			robotstxtConfig.setUserAgentName(UATool.getUA());
			RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
			controller = new CrawlController(config, pageFetcher, robotstxtServer);

			for (String seed : SeedConfig.getSeeds(className)) {
				controller.addSeed(seed);
			}

			controller.start(new WebCrawlerFactory<WebCrawler>() {
				@Override
				public WebCrawler newInstance() throws Exception {
					return (WebCrawler) Class.forName(packageName + "." + className).newInstance();
				}
			}, numberOfCrawlers);
		} else {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("options", options);
		}

	}

}
