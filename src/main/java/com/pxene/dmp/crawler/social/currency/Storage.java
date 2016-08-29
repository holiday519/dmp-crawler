package com.pxene.dmp.crawler.social.currency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class Storage
{
    BlockingQueue<Product> queues = new LinkedBlockingQueue<Product>();
    
    
    /**
     * 生产：向阻塞队列中尾部添加产品，当队列为满时调用者的线程被阻塞，直到不为满时再被唤醒。
     * @param product 产品
     * @throws InterruptedException
     */
    public void push(Product product) throws InterruptedException
    {
        queues.put(product);
    }
    
    /**
     * 消费：从阻塞队列的首部取出产品，当队列为空时调用者的线程被阻塞，直到不为空时再被唤醒。如果等待10分钟仍无法从队列取得产品，则返回null。
     * @return 产品
     * @throws InterruptedException
     */
    public Product pop() throws InterruptedException
    {
        return queues.poll(10, TimeUnit.MINUTES);
    }
}
