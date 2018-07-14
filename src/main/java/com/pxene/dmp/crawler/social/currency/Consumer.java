package com.pxene.dmp.crawler.social.currency;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.crawler.social.worker.WXEntityImporter;


public class Consumer implements Runnable
{
    private static Logger logger = LogManager.getLogger(Consumer.class.getName());
    
    private String name;
    private Storage storage = null;
    private Resource resource = null;
    private CountDownLatch countDownLatch = null;
    private WXEntityImporter importer = new WXEntityImporter();
    
    public Consumer(String name, Storage storage)
    {
        this.name = name;
        this.storage = storage;
    }
    public Consumer(String name, Storage storage, Resource resource, CountDownLatch countDownLatch)
    {
        this(name, storage);
        this.resource = resource;
        this.countDownLatch = countDownLatch;
    }
    
    
    @Override
    public void run()
    {
        try
        {
            while (true)
            {
                Product product = storage.pop();
                
                // 如果是“毒药丸”，则放回队列（杀死其他消费者），然后结束自己
                if (product.getId() == -1)
                {
                    storage.push(product);
                    System.out.println(name + "finished!");
                    break;
                }
                
                logger.info(name + "正在录入[" + product + "]的数据...");
                importer.doImport(product, resource);
            }
        }    
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally 
        {
            countDownLatch.countDown();
        }
    }
}