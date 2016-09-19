package com.pxene.dmp.main;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.pxene.dmp.common.CookieList;
import com.pxene.dmp.common.CrawlerConfig;
import com.pxene.dmp.common.CrawlerConfig.LoginConf;
import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.common.SeedParser;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.ms.Crawler4ZhishikuFJ;

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
		String userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36";
		// 命令行配置
		Options options = new Options();
		options.addOption("crawlPages", true, "input class name of crawler");
		options.addOption("runner", true,
				"input full class name of Custom crawler");
		CommandLineParser parser = new BasicParser();
		CommandLine line = parser.parse(options, args);

		if (line.hasOption("crawlPages")) {
			// className:auto.Crawler4Autohome
			final String className = line.getOptionValue("crawlPages");
			CrawlConfig config = new CrawlConfig();
			// 读取配置文件
			String confPath = "/"
					+ (packageName + "." + className).replace(".", "/")
					+ ".json";
			CrawlerConfig conf = CrawlerConfig.load(confPath);
			LoginConf loginConf = conf.getLoginConf();
			// 初始化cookie
			CookieList.init(packageName + "." + className, loginConf);

			if (loginConf.isEnable()) {
				List<AuthInfo> infos = new ArrayList<AuthInfo>();
				// AuthInfo info = new BasicAuthInfo(loginConf.getUsername(),
				// loginConf.getPassword(),
				// loginConf.getUrl());
				AuthInfo info = new FormAuthInfo(loginConf.getUsername(),
						loginConf.getPassword(), loginConf.getUrl(),
						loginConf.getUsrkey(), loginConf.getPwdkey());
				infos.add(info);
				config.setAuthInfos(infos);
			}
			config.setUserAgentString(userAgent);

			// 抓取深度
			 config.setMaxDepthOfCrawling(5);
			// 最大网页数
			 config.setMaxPagesToFetch(100000);
			config.setCrawlStorageFolder(crawlStorageFolder);
			config.setSocketTimeout(20000);
			config.setConnectionTimeout(20000);
			//可恢复的爬取数据(如果爬虫意外终止或者想要实现增量爬取，可以通过设置这个属性来进行爬取数据)
			config.setResumableCrawling(true);

			PageFetcher pageFetcher = new PageFetcher(config);
			RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
			// 默认关闭robot协议
			robotstxtConfig.setEnabled(false);
			robotstxtConfig.setUserAgentName(userAgent);
			RobotstxtServer robotstxtServer = new RobotstxtServer(
					robotstxtConfig, pageFetcher);
			CrawlController controller = new CrawlController(config,
					pageFetcher, robotstxtServer);
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
		else if (line.hasOption("runner")) 
		{
			// without param, e.g. -runner com.pxene.dmp.crawler.stock.Crawler410jqka
		    // with    param, e.g. -runner com.pxene.dmp.crawler.stock.Crawler410jqka[p1%3dv1%26p2%3dv2%26p3%3dmp3%3bmp4]
			final String runner = line.getOptionValue("runner");
			System.out.println("runner: " + runner);
			
			String className = runner;
			String[] lineArgs = null;
			if (runner != null && !"".equals(runner) && runner.contains("["))
			{
			    className = runner.substring(0, runner.indexOf("["));
		        String params = StringUtils.regexpExtract(runner, "\\w+\\[(.*)?\\]").trim();
		        
		        if (params != null && params.equals(""))
		        {
		            System.out.println("-runner's param is incorrect.");
		            System.exit(-1);
		        }
		        
		        System.out.println("Recevied runner param is " + params);
		        try
		        {
		            params = URLDecoder.decode(params, "utf-8");
		            lineArgs = params.split("&");
		        }
		        catch (UnsupportedEncodingException e)
		        {
		            params = "";
		        }
			}

            Class<?> c = Class.forName(className);
			IPageCrawler pageCrawler = (IPageCrawler) c.newInstance();
			
			Method doCrawlMethod = c.getDeclaredMethod("doCrawl", String[].class);
            if (lineArgs != null)
            {
                doCrawlMethod.invoke(pageCrawler, new Object[]{ lineArgs });
            }
            else
            {
                doCrawlMethod.invoke(pageCrawler, new Object[]{ null });
            }
		} 
		else 
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("options", options);
		}
	}
}