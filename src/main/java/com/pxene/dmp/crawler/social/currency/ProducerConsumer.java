package com.pxene.dmp.crawler.social.currency;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ProducerConsumer
{
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService service = Executors.newFixedThreadPool(50);
        
        Storage storage = new Storage();
        
        Resource resource = new Resource();
        
        
        System.out.println("提交任务：生产者.");
        Producer wxMetaParamProducer = new Producer("生产者", storage);
        service.submit(wxMetaParamProducer);
        
        
        int threadNums = 100;
        for (int i = 1; i <= threadNums; i++)
        {
            System.out.println("提交任务：消费者" + i + "号.");
            Consumer wxMetaParamConsumer = new Consumer("消费者" + i + "号", storage, resource);
            service.submit(wxMetaParamConsumer);
        }
        
        
        service.shutdown();
    }
}
