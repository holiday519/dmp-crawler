package com.pxene.dmp.main;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.pxene.dmp.common.CookieList;
import com.pxene.dmp.common.CrawlerConfig;
import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.common.CrawlerConfig.LoginConf;
import com.pxene.dmp.common.SeedParser;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.CrawlController.WebCrawlerFactory;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.crawler.authentication.AuthInfo;
import edu.uci.ics.crawler4j.crawler.authentication.FormAuthInfo;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CrawlerManager {

	public static void main(String[] args) throws Exception {
		// 基本配置
		final String packageName = "com.pxene.dmp.crawler";
		String crawlStorageFolder = "temp";
		int numberOfCrawlers = 50;
		// 默认的ua
		String userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
		// 命令行配置
		Options options = new Options();
		options.addOption("crawlPages", true, "input class name of crawler");
		options.addOption("runnerClassName", true, "input full class name of Custom crawler");
		CommandLineParser parser = new BasicParser();
		CommandLine line = parser.parse(options, args);

		if (line.hasOption("crawlPages")) {
			// className:auto.Crawler4Autohome
			final String className = line.getOptionValue("crawlPages");
			CrawlConfig config = new CrawlConfig();
			// 读取配置文件
			String confPath = "/" + (packageName + "." + className).replace(".", "/") + ".json";
			CrawlerConfig conf = CrawlerConfig.load(confPath);
			LoginConf loginConf = conf.getLoginConf();
			// 初始化cookie
			CookieList.init(packageName + "." + className, loginConf);
			
			if (loginConf.isEnable()) {
				List<AuthInfo> infos = new ArrayList<AuthInfo>();
//				AuthInfo info = new BasicAuthInfo(loginConf.getUsername(), loginConf.getPassword(),
//						loginConf.getUrl());
				AuthInfo info = new FormAuthInfo(loginConf.getUsername(), 
						loginConf.getPassword(), loginConf.getUrl(), loginConf.getUsrkey(), 
						loginConf.getPwdkey());
				infos.add(info);
				config.setAuthInfos(infos);
			}
			config.setUserAgentString(userAgent);
			
			// 抓取深度
//			config.setMaxDepthOfCrawling(5);
			// 最大网页数
//			config.setMaxPagesToFetch(10000);
			
			config.setCrawlStorageFolder(crawlStorageFolder);
			config.setSocketTimeout(20000);
			config.setConnectionTimeout(20000);

			PageFetcher pageFetcher = new PageFetcher(config);
			RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
			// 默认关闭robot协议
			robotstxtConfig.setEnabled(false);
			robotstxtConfig.setUserAgentName(userAgent);
			RobotstxtServer robotstxtServer = new RobotstxtServer(
					robotstxtConfig, pageFetcher);
			CrawlController controller = new CrawlController(config, pageFetcher,
					robotstxtServer);
			// 加载种子
			for (String seed : conf.getSeeds()) {
				for (String s : SeedParser.parse(seed)) {
					controller.addSeed(s);
				}
			}

			controller.start(new WebCrawlerFactory<WebCrawler>() {
				@Override
				public WebCrawler newInstance() throws Exception {
					return (WebCrawler) Class.forName(
							packageName + "." + className).newInstance();
				}
			}, numberOfCrawlers);
		}
		else if (line.hasOption("runnerClassName"))
		{
		    // e.g. -runnerClassName com.pxene.dmp.crawler.stock.Crawler410jqka
            final String runnerClassName = line.getOptionValue("runnerClassName");
            System.out.println(runnerClassName);
            
            Class<?> c = Class.forName(runnerClassName);
            IPageCrawler pageCrawler = (IPageCrawler) c.newInstance();
            
            Method doCrawlMethod = c.getDeclaredMethod("doCrawl");
            doCrawlMethod.invoke(pageCrawler);
		}
		else 
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("options", options);
		}
	}
}