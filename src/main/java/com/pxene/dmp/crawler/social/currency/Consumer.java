package com.pxene.dmp.crawler.social.currency;

import java.util.concurrent.Callable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.crawler.social.worker.WXEntityImporter;

public class Consumer implements Callable<Boolean>
{
    private static Logger logger = LogManager.getLogger(Consumer.class.getName());
    
    private String name;
    private Storage storage = null;
    private Resource resource = null;
    private WXEntityImporter importer = new WXEntityImporter();
    
    public Consumer(String name, Storage storage)
    {
        this.name = name;
        this.storage = storage;
    }
    public Consumer(String name, Storage storage, Resource resource)
    {
        this(name, storage);
        this.resource = resource;
    }
    
    
    @Override
    public Boolean call() throws InterruptedException
    {
        while (true)
        {
            Product product = storage.pop();
            
            if (product == null)
            {
                logger.info(name + " ==> 已等待10分钟依然无法获得数据，退出.");
                return Boolean.FALSE;
            }
            else
            {
                logger.info(name + "正在录入[" + product + "]的数据...");
                importer.doImport(product, resource);
            }
        }
    }
}