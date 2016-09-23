package com.pxene.dmp.crawler.social.jms;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.omg.CORBA.PRIVATE_MEMBER;

import com.pxene.dmp.crawler.social.currency.ProducerConsumer;

public class JMSConsumer extends BoundaryBase
{
    private String clientId = "dmp_jms_default_client";
    private String subName = "dmp_jms_default_sub";
    private String topicName = "dmp_jms_default_topic";
    
    
    public static void main(String[] args)
    {
        new JMSConsumer().consume();
    }
    
    public JMSConsumer() {}
    public JMSConsumer(String clientId, String subName, String topicName)
    {
        super();
        this.clientId = clientId;
        this.subName = subName;
        this.topicName = topicName;
    }



    public void consume()
    {
        Connection connection = null;
        
        Session session;
        
        Destination destination;
        
        MessageConsumer messageConsumer;
        
        try
        {
            connection = getConnection();
            connection.setClientID(clientId);
            connection.start();
            
            session = getSession(connection);
            
            destination = createTopic(session);
            
            //messageConsumer = session.createConsumer(destination, "dmp03");
            messageConsumer = session.createDurableSubscriber((Topic) destination, subName);
            
            while (true)
            {
                MapMessage message = (MapMessage) messageConsumer.receive(100000);
                if (message != null && !"".equals(message))
                {
                    String dateStr = message.getString("date_str");
                    String partitionSource = message.getString("partition_source");
                    System.out.println("收到的消息: " + dateStr + ", " + partitionSource);
                    
                    
                    String[] args = new String[]{dateStr, partitionSource};
                    /*try
                    {
                        System.out.println("启动微信爬取程序……");
                        ProducerConsumer.main(args);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                    catch (ExecutionException e)
                    {
                        e.printStackTrace();
                    }*/
                }
                else
                {
                    break;
                }
            }
            
        }
        catch (JMSException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    protected String getTopicName()
    {
        return topicName;
    }
}
