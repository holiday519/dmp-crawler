package com.pxene.dmp.crawler.social.currency;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.crawler.social.worker.WXMetaDataGenerator;

public class Producer implements Callable<Boolean>
{
    private static Logger logger = LogManager.getLogger(Producer.class.getName());
    
    private String name;
    private Storage storage = null;
    private String dateStr;
    private String partitionSource;
    private WXMetaDataGenerator generator = new WXMetaDataGenerator();
    
    
    public Producer(String name, Storage storage, String dateStr, String partitionSource)
    {
        this.name = name;
        this.storage = storage;
        this.dateStr = dateStr;
        this.partitionSource = partitionSource;
    }
    
    
    @Override
    public Boolean call()
    {
        logger.info(name + "已启动.");
        try
        {
            List<Product> products = generator.generate(dateStr, partitionSource);
            for (Product product : products)
            {
                storage.push(product);
            }
            return Boolean.TRUE;
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return Boolean.FALSE;
        }
    }
}
