package com.pxene.dmp.crawler.social.utils;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class ProxyValidator
{
    
    private static final String DEFAULT_REQUEST_URL = "http://www.baidu.com";
    private static final int CONNECT_TIMEOUT = 300;
    private static final int SOCKET_TIMEOUT = 150;
    
    private static final String REDIS_HOST = "dmp08";
    private static final int REDIS_PORT = 7000;
    
    
    public static CloseableHttpClient createSSLClientDefault()
    {
        try
        {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy()
            {
                // 信任所有
                public boolean isTrusted(X509Certificate[] chain, String authType)
                {
                    return true;
                }
            }).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext);
            return HttpClients.custom().setSSLSocketFactory(sslsf).build();
        }
        catch (KeyManagementException e)
        {
            e.printStackTrace();
        }
        catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }
        catch (KeyStoreException e)
        {
            e.printStackTrace();
        }
        return HttpClients.createDefault();
    }
    public static void main(String[] args)
    {
        String url = "http://p.3.cn/prices/get?type=1&skuid=J_10203691212";
        /*
        String proxyHost = "117.169.4.42";
        int proxyPort = 81;
        String proxyHost = "60.21.209.114";
        int proxyPort = 8080;
         */        
        String proxyHost = "117.169.4.42";
        int proxyPort = 81;
        String html = "";
        CloseableHttpClient closeableHttpClient = createSSLClientDefault();
        
        HttpGet httpGet = new HttpGet(url);
        if (proxyHost != null && !"".equals(proxyHost) && proxyPort != 0)
        {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort); 
            RequestConfig config = RequestConfig.custom().setProxy(proxy).build();
            httpGet.setConfig(config);
        }
        
        CloseableHttpResponse response;
        try
        {
            response = closeableHttpClient.execute(httpGet);
            html = EntityUtils.toString(response.getEntity(), "UTF-8");
            response.close();
            closeableHttpClient.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        
        System.out.println(html);
        
    }

    public static void main1(String[] args)
    {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(50000);
        config.setMaxIdle(100);
        config.setMaxWaitMillis(10 * 1000);
        config.setTestOnBorrow(true);
        JedisPool jedisPool = new JedisPool(config, REDIS_HOST, REDIS_PORT);
        Jedis jedis = jedisPool.getResource();
        jedis.select(10);
        String randomKey = jedis.randomKey();
        System.out.println("Redis randomKey: " + randomKey);
        
        String proxyHost = "";
        Integer proxyPort = 0;
        for (String hkey : jedis.hkeys(randomKey))
        {
            String tmpVal = jedis.hget(randomKey, hkey);
            if ("host".equals(hkey))
            {
                proxyHost= tmpVal;
            }
            else if ("port".equals(hkey))
            {
                proxyPort = Integer.valueOf(tmpVal);
            }
        }
        System.out.println(proxyHost + " is " + checkProxy(proxyHost, proxyPort, null, null, null));
        
        jedisPool.close();
        
    }
    
    public static boolean checkProxy(String proxyHost, int proxyPort)
    {
        return checkProxy(proxyHost, proxyPort, null, null, null);
    }
    public static boolean checkProxy(String proxyHost, int proxyPort, String proxyProtocal, String proxyUsername, String proxyPassword)
    {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        
        int statusCode = 0;
        
        try
        {
            HttpClientBuilder builder = HttpClients.custom();
            builder.setProxy(new HttpHost(proxyHost, proxyPort, proxyProtocal));
            
            // 如果代理服务器需要验证，设置用户名和密码
            if (proxyUsername != null && !"".equals(proxyUsername))
            {
                AuthScope authscope = new AuthScope(proxyHost, proxyPort);
                Credentials credentials = new UsernamePasswordCredentials(proxyUsername, proxyPassword);
                
                CredentialsProvider credsProvider = new BasicCredentialsProvider();
                credsProvider.setCredentials(authscope, credentials);
                builder.setDefaultCredentialsProvider(credsProvider);
            }
            
            httpClient = builder.build();
            
            // 配置连接超时时间和套接字超时时间
            RequestConfig config = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
            
            HttpGet httpGet = new HttpGet(DEFAULT_REQUEST_URL);
            httpGet.setConfig(config);
            
            response = httpClient.execute(httpGet);
            
            statusCode = response.getStatusLine().getStatusCode();
        }
        catch (Exception e)
        {
            //e.printStackTrace();
            return false;
        }
        finally 
        {
            if(response != null)
            {
                try
                {
                    response.close();
                }
                catch (IOException e)
                {
                    //e.printStackTrace();
                    return false;
                }
            }
            if(httpClient != null)
            {
                try
                {
                    httpClient.close();
                }
                catch (IOException e)
                {

                    e.printStackTrace();
                    return false;
                }
            }
        }
        
        if (statusCode == 200)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
}
