package com.pxene.dmp.crawler.social.application;

import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.crawler.social.currency.ProducerConsumer;

public class Application implements IPageCrawler
{

    @Override
    public void doCrawl(String[] args) throws Exception
    {
        if (args != null)
        {
            for (String string : args)
            {
                System.out.println("Param ==> " + string);
            }
        }
        ProducerConsumer.main(args);
    }
    
}
