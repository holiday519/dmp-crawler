package com.pxene.dmp.crawler.social.jms;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;


public class JMSProducer extends BoundaryBase
{
    private static final String TOPIC_NAME = "demoTopic";
    
    
    public static void main(String[] args)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2016, 8, 3, 18, 7, 0);
        
        new JMSProducer().produce(calendar.getTime(), "01");
    }
    
    /**
     * 
     * @param date
     * @param partitionSource 1:国柱，2：晓语
     */
    public void produce(Date date, String partitionSource)
    {
        Connection connection = null;
        
        Session session = null;
        
        Destination destination;
        
        MessageProducer messageProducer;
        
        try
        {
            connection = getConnection();
            
            connection.start();
            
            session = getSession(connection);
            
            destination = createTopic(session);
            
            messageProducer = session.createProducer(destination);
            
            String opDateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
            sendMessage(session, messageProducer, opDateStr, partitionSource);
            
            session.commit();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        finally 
        {
            if (session != null)
            {
                try
                {
                    session.close();
                }
                catch (JMSException e)
                {
                    e.printStackTrace();
                }
            }
            if (connection != null)
            {
                try
                {
                    connection.close();
                }
                catch (JMSException e)
                {
                    e.printStackTrace();
                }
            }
        }
        
    }
    
    private static void sendMessage(Session session, MessageProducer messageProducer, String opDateStr, String partitionSource) throws JMSException
    {
        // 创建一条Map消息
        MapMessage message = session.createMapMessage();
        message.setString("date_str", opDateStr);
        message.setString("partition_source", partitionSource);
        System.out.println("发送消息：Activemq 发送消息：" + opDateStr + ", " + partitionSource);
        
        // 通过消息生产者发出消息
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        messageProducer.send(message);
    }

    @Override
    protected String getTopicName()
    {
        return TOPIC_NAME;
    }

}
