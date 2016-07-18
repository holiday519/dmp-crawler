package com.pxene.dmp.crawler.social.currency;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.crawler.social.worker.WXEntityImporter;

public class Consumer implements Runnable
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
    public void run()
    {
        while (true)
        {
            try
            {
                Product product = storage.pop();
                
                logger.info(name + "正在录入[" + product + "]的数据...");
                importer.doImport(product, resource);
                
            }
            catch (InterruptedException exception)
            {
                exception.printStackTrace();
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
            finally
            {
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }
}