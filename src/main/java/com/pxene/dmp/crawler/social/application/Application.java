package com.pxene.dmp.crawler.social.application;

import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.crawler.social.currency.ProducerConsumer;

public class Application implements IPageCrawler
{

    @Override
    public void doCrawl() throws Exception
    {
        ProducerConsumer.main(null);
    }
    
}
