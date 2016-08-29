package com.pxene.dmp.crawler.social.currency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class ProducerConsumer
{
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService service = Executors.newFixedThreadPool(500);
        
        Storage storage = new Storage();
        
        Resource resource = new Resource();
        
        
        System.out.println("提交任务：生产者.");
        Producer wxMetaParamProducer = new Producer("生产者", storage);
        Future<Boolean> producerFuture = service.submit(wxMetaParamProducer);
        
        int threadNums = 100;
        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (int i = 1; i <= threadNums; i++)
        {
            System.out.println("提交任务：消费者" + i + "号.");
            Consumer wxMetaParamConsumer = new Consumer("消费者" + i + "号", storage, resource);
            Future<Boolean> consumerFuture = service.submit(wxMetaParamConsumer);
            futures.add(consumerFuture);
        }
        
        if (producerFuture.get())
        {
            System.out.println("===> 生产者已完成全部生产任务.");
        }
        for (Future<Boolean> future : futures)
        {
            if (!future.get())
            {
                System.out.println("===> 消费者超时，被迫退出.");
            }
        }
        
        
        service.shutdown();
    }
}
