package com.pxene.dmp.crawler.social.currency;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Connection;


public class ProducerConsumer
{
    private static Resource resource = new Resource();
    private static final int THREAD_NUM = 100;
    
    
    public static void main(String[] args) throws IOException,  ExecutionException
    {
        ExecutorService service = Executors.newFixedThreadPool(50);
        
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
        
        Storage storage = new Storage();
        
        if (args == null || args.length != 2)
        {
            System.out.println("<fatal error> Input param is incorrect.");
            System.exit(-1);
        }
        
        
        Connection connection = resource.getHBaseConnection();
        if (connection == null || connection.isClosed())
        {
            System.err.println("HBase client connection is not ready.");
            System.exit(-1);
        }
        
        String dateStr = args[0];
        String partitionSource = args[1];
        
        System.out.println("提交任务：生产者.");
        Producer wxMetaParamProducer = new Producer("生产者", storage, dateStr, partitionSource);
        Future<Boolean> producerFuture = service.submit(wxMetaParamProducer);
        
        
        for (int i = 1; i <= THREAD_NUM; i++)
        {
            System.out.println("提交任务：消费者" + i + "号.");
            Consumer wxMetaParamConsumer = new Consumer("消费者" + i + "号", storage, resource, countDownLatch);
            service.execute(wxMetaParamConsumer);
        }
        
        try
        {
            if (producerFuture.get())
            {
                System.out.println("===> 生产者已完成全部生产任务.");
            }
            
            countDownLatch.await();
            System.out.println("倒计时计数器已结束。");
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        
        service.shutdown();
        
        resource.getHBaseConnection().close();

        if (countDownLatch.getCount() == 0)
        {
            System.out.println("线程池已关闭，主线程结束。");
            System.exit(1);
        }
    }
}
