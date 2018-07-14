package com.pxene.dmp.crawler;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


public class SimpleWebCrawler extends BaseCrawler
{
    private static final int MAX_REPEAT_COUNTS = 3;
    private Logger logger = null;
    private static Set<String> failedURLs = new HashSet<String>();
    
    
    public void setLogger(Logger logger)
    {
        this.logger = logger;
    }
    
    public Set<String> getFailedURLSet()
    {
        return failedURLs;
    }

    
    protected SimpleWebCrawler(String confPath)
    {
        super(confPath);
    }
    
    
    public Document doRepeatableParse(String url)
    {
        return doRepeatableParse(url, MAX_REPEAT_COUNTS, null);
    }
    public Document doRepeatableParse(String url, String proxy)
    {
        return doRepeatableParse(url, MAX_REPEAT_COUNTS, proxy);
    }
    public Document doRepeatableParse(String url, int repeatCounts, String proxy)
    {
        logger.debug("正在进行页面爬取，URL：" + url + ", [proxy info]" + proxy);
        
        int redo = 0;
        while (redo < MAX_REPEAT_COUNTS)
        {
            try
            {
                Connection connection = Jsoup.connect(url).userAgent("Chrome");
                if (proxy != null && proxy.contains(":") && redo == 0)
                {
                    String[] tmpArr = proxy.split(":");
                    if (tmpArr != null && tmpArr.length == 2)
                    {
                        connection.proxy(tmpArr[0], Integer.valueOf(tmpArr[1]));
                    }
                }
                Document document = connection.get(); 
                return document;
            }
            catch (Exception e)
            {
                redo++;
                logger.debug("获取URL" + url + "失败，正在进行第" + redo + "次尝试...");
                continue;
            }
        }
        
        if (redo >= MAX_REPEAT_COUNTS)
        {
            failedURLs.add(url);
            logger.debug("已完成" + redo + "次重试，全部失败，放弃URL" + url + ".");
        }
        return null;
    }
}
