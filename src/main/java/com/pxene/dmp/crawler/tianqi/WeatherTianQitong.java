package com.pxene.dmp.crawler.tianqi;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.gpsspg.BSPojo;

public class WeatherTianQitong implements IPageCrawler
{
    private static Logger logger = LogManager.getLogger(WeatherTianQitong.class.getName());
    
    private static final String HBASE_TABLE_NAME = "t_weather_citycode";
    
    private static final String HBASE_ROWKEY_PREFIX = "000050013001";
    
    private static final String HBASE_COLUMN_FAMILY = "info";
    
    /**
     * Hive JDBC驱动
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    
    /**
     * Hive 连接URL
     */
    private static final String HIVE_URL = "jdbc:hive2://dmp01:10000/db_telecom";
    
    /**
     * Hive 用户名
     */
    private static final String HIVE_USERNAME = "ningyu";
    
    /**
     * API URL模板
     */
    private static final String REQUEST_URL_TEMPLATE = "http://forecast.sina.cn/app/update.php?city={0}&timezone=GMT%2B8&resolution=640%2A960&pv=5.159&device=iPhone&conn=1&carrier=2&sv=9.340&ts=1476329728.403958&sign=f6f949ea2e6331fa1c36a99af6552087&tqt_userid=182160209&uid=bb71576e4fc186834aa8f682d4a95eb9e257ccd5&weibo_aid=01ArQreAOc6LUXVA22nMaDUSC2iEqXWV6xAugj6ijqA6JGLtI.&idfa=5E17473C-B8A9-4C38-983B-D2600B62D349&api_key=517276d5c1d3c&pid=free&lang=zh-Hans&pt=3010&device_id=ddb6e9b51110046cda9f916a47962f1578427347&pd=tq";
    
    /**
     * HttpClient 请求 Referer
     */
    private static final String REQUEST_REFERER = "http://www.gpsspg.com/bs.htm"; 
    
    /**
     * HttpClient 请求 User-Agent
     */
    private static final String REQUEST_UA = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36 Core/1.47.640.400 QQBrowser/9.4.8309.400"; 

    /**
     * HttpClient 请求 超时时间
     */
    private static final int REQUEST_TIMEOUT = 2000;
    
    
    @Override
    public void doCrawl(String[] args) throws Exception
    {
        loadAndSave("1=1");
    }

    
    public static void main(String[] args) throws IOException
    {
        WeatherTianQitong tianQitong = new WeatherTianQitong();
        String url = MessageFormat.format(REQUEST_URL_TEMPLATE, "WMXX1644");
        System.out.println(url);
        String apiResult = tianQitong.invokeAPI(url, null);
        String cityName = tianQitong.geCityName(apiResult);
        System.out.println(apiResult);
        System.out.println(cityName);
    }
    
    
    public void loadAndSave(String condStr) throws IOException, SQLException
    {
        Connection con = null;
        try
        {
            Class.forName(driverName);
            
            con = DriverManager.getConnection(HIVE_URL, HIVE_USERNAME, "");
            
            Statement stmt = con.createStatement();
            
            logger.info("正在查询Hive表：‘ee_weathercity_v03’，请稍候......");
            
            String sql = "select distinct(city) from ee_weathercity_v03 where domain='00050013001' and " + condStr;
            logger.info("查询SQL：" + sql);
            ResultSet res = stmt.executeQuery(sql);
            
            logger.debug("Scan Result:");
            logger.debug("city");
            
            String cityCode = "";
            
            int i = 0;
            while (res.next()) 
            {
                cityCode = res.getString(1);
                
                String url = MessageFormat.format(REQUEST_URL_TEMPLATE, cityCode);
                String apiResult = invokeAPI(url, null);
                String cityName = geCityName(apiResult);
                
                if (cityName != null && !"".equals(cityName))
                {
                    Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
                    
                    String rowKey = HBASE_ROWKEY_PREFIX + "_" + cityCode;
                    
                    HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "code", Bytes.toBytes(cityCode));
                    HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "name", Bytes.toBytes(cityName));
                    HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "parent_code", Bytes.toBytes(""));
                    HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "parent_name", Bytes.toBytes(""));
                    HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "full_name", Bytes.toBytes(""));
                    
                    HBaseTools.insertIntoHBase(HBASE_TABLE_NAME, preparedData);
                    i++;
                }
                else
                {
                    logger.error("<= TONY => cityName parse error: [cityCode=" + cityCode + "].");
                }
                
                Thread.sleep(2000);
            }
            logger.info("<= TONY => Total Hive amount: " + i);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
        finally 
        {
            con.close();
        }
    }
    
    
    private String geCityName(String apiResult)
    {
        // TODO 自动生成的方法存根
        return null;
    }


    /**
     * 调用http://www.gpsspg.com/bs.htm的基站查询接口，返经纬度、地址、街道信息。
     * @param url       包含请求参数的API URL
     * @param proxy     代理IP地址，格式："ip:port"，如果不需要使用代理则传参null或""
     * @return
     */
    private String invokeAPI(String url, String proxy)
    {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;
        try
        {
            //httpGet.setHeader("Referer", REQUEST_REFERER);
            //httpGet.setHeader("User-Agent", REQUEST_UA);
            
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(REQUEST_TIMEOUT).setConnectionRequestTimeout(REQUEST_TIMEOUT).setSocketTimeout(REQUEST_TIMEOUT).build();
            httpGet.setConfig(requestConfig);
            
            if (proxy != null && !"".equals(proxy))
            {
                String[] tmpArr = proxy.split(":");
                if (tmpArr != null && tmpArr.length == 2)
                {
                    HttpHost proxyHost = new HttpHost(tmpArr[0], Integer.parseInt(tmpArr[1]), "http");  
                    RequestConfig config = RequestConfig.custom().setProxy(proxyHost).build();
                    httpGet.setConfig(config);
                }
            }
            
            response = httpclient.execute(httpGet);
            
            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            
            if (statusLine != null && statusLine.getStatusCode() >= 300)
            {
                throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
            }
            
            if (entity == null)
            {
                throw new ClientProtocolException("response contains no content");
            }
            
            return EntityUtils.toString(entity);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (response != null)
            {
                try
                {
                    response.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
    
}
