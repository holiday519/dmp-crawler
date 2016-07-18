package com.pxene.dmp.crawler.social.currency;

import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.crawler.social.utils.StringUtils;
import com.pxene.dmp.crawler.social.worker.WXMetaDataGenerator;

public class Producer implements Runnable
{
    private static Logger logger = LogManager.getLogger(Producer.class.getName());
    
    private String name;
    private Storage storage = null;
    private WXMetaDataGenerator generator = new WXMetaDataGenerator();
    
    
    public Producer(String name, Storage storage)
    {
        this.name = name;
        this.storage = storage;
    }
    
    
    @Override
    public void run()
    {
        logger.info(name + "已启动.");
        try
        {
            List<Product> products = generator.generate(StringUtils.getYestodayStr(""));
            for (Product product : products)
            {
                storage.push(product);
            }
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }
}
