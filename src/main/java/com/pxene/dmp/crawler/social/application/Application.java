package com.pxene.dmp.crawler.social.application;

import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.crawler.social.currency.ProducerConsumer;
import com.pxene.dmp.crawler.social.jms.JMSConsumer;

public class Application implements IPageCrawler
{

    @Override
    public void doCrawl(String[] args) throws Exception
    {
        if (args != null)
        {
            String mode = null;
            String clientId = null;
            String subName = null;
            String topicName = null;
            
            for (String arg : args)
            {
                System.out.println("Param ==> " + arg);
                if (arg != null && !"".equals(arg) && arg.contains("="))
                {
                    String[] kv = arg.split("=");
                    if (kv == null || kv.length != 2)
                    {
                        continue;
                    }
                    
                    if ("mode".equals(kv[0]))
                    {
                        mode = kv[1];
                    }
                    if ("clientId".equals(kv[0]))
                    {
                        clientId = kv[1];
                    }
                    if ("subName".equals(kv[0]))
                    {
                        subName = kv[1];
                    }
                    if ("topicName".equals(kv[0]))
                    {
                        topicName = kv[1];
                    }
                }
            }
            
            if (mode != null && mode.equals("jms"))
            {
                JMSConsumer consumer = new JMSConsumer(clientId, subName, topicName);
                consumer.consume();
            }
            else
            {
                ProducerConsumer.main(args);
            }
        }
    }
    
}
