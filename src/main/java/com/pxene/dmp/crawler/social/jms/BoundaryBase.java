package com.pxene.dmp.crawler.social.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public abstract class BoundaryBase
{
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    private static final String BROKEURL = "tcp://192.168.3.176:61616"; // ActiveMQConnection.DEFAULT_BROKER_URL;
    
    
    protected Connection getConnection() throws JMSException
    {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BoundaryBase.USERNAME, BoundaryBase.PASSWORD, BoundaryBase.BROKEURL);
        
        return connectionFactory.createConnection();
    }
    
    protected Session getSession(Connection connection) throws JMSException
    {
        return connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
    }
    
    protected Destination createTopic(Session session) throws JMSException
    {
        return session.createTopic(getTopicName());
    }

    protected abstract String getTopicName();
}
