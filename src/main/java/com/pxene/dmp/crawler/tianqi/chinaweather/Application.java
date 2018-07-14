package com.pxene.dmp.crawler.tianqi.chinaweather;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

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
import org.json.JSONObject;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.crawler.tianqi.AreaPojo;

public class Application implements IPageCrawler
{
    private static Logger logger = LogManager.getLogger(Application.class.getName());
    
    private static final String HBASE_TABLE_NAME = "t_weather_citycode";
    
    private static final String HBASE_COLUMN_FAMILY = "info";
    
    private static final String REQUEST_URL_TEMPLATE_PROVINCE = "http://bj.weather.com.cn/data/city3jdata/provshi/{0}.html";
    
    private static final String REQUEST_URL_TEMPLATE_CITY = "http://bj.weather.com.cn/data/city3jdata/station/{0}.html";

    private static final String REQUEST_REFERER = "http://bj.weather.com.cn/";

    private static final String REQUEST_UA = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36 Core/1.47.640.400 QQBrowser/9.4.8309.400";

    private static final int REQUEST_TIMEOUT = 5000;

    private static final int SLEEP_MILLIS = 3000;

    
    
    @Override
    public void doCrawl(String[] args) throws Exception
    {
        //String[] provices = {"10101", "10106"};
        String[] provices = {"10101", "10102", "10103", "10104", "10105", "10106", "10107", "10108", "10109", "10110", "10111", "10112", "10113", "10114", "10115", "10116", "10117", "10118", "10119", "10120", "10121", "10122", "10123", "10124", "10125", "10126", "10127", "10128", "10129", "10130", "10131", "10132", "10133", "10134"};
        
        Map<String, AreaPojo> areaMap = new HashMap<String, AreaPojo>();
        
        int i = 0;
        for (String provice : provices)
        {
            String provinceApiResult = requestRequest(REQUEST_URL_TEMPLATE_PROVINCE, provice);
            Thread.sleep(SLEEP_MILLIS);
            
            if (provinceApiResult != null && !"".equals(provinceApiResult))
            {
                JSONObject provinceResultObj = new JSONObject(provinceApiResult);
                for (String key : JSONObject.getNames(provinceResultObj))
                {
                    String cityCode = provice + key;
                    String cityName = provinceResultObj.getString(key);
                    
                    String cityApiResult = requestRequest(REQUEST_URL_TEMPLATE_CITY, cityCode);
                    Thread.sleep(SLEEP_MILLIS);
                    
                    if (cityApiResult != null && !"".equals(cityApiResult))
                    {
                        JSONObject cityResultObj = new JSONObject(cityApiResult);
                        for (String k : JSONObject.getNames(cityResultObj))
                        {
                            AreaPojo areaPojo = new AreaPojo(cityCode + k, cityResultObj.getString(k), cityCode, cityName);
                            areaMap.put(cityCode + k, areaPojo);
                            //System.out.println(areaPojo);
                            //System.out.println(cityName + "@" + cityCode + k + " *** " + cityResultObj.getString(k));
                        }
                    }
                }
                
                i++;
            }
            System.out.println("---------------------------\n\n");
            
        }
        System.out.println("Proince amount: " + i);
        
        
        String[] appCodes = {"00050012", "00050014", "00050018", "00050019"};
        for (String appCode : appCodes)
        {
            System.out.println("正在录入App：" + appCode + "的数据...");
            insertIntoHBase(appCode, areaMap);
        }
        
        System.out.println(areaMap.size());
    }


    private void insertIntoHBase(String appCode, Map<String, AreaPojo> areaMap) throws IOException
    {
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        for (Map.Entry<String, AreaPojo> entry : areaMap.entrySet())
        {
            String rowKey = appCode + "_" + entry.getKey();
            AreaPojo areaPojo = entry.getValue();
            
            String code = areaPojo.getCode();
            String name = areaPojo.getName();
            String parentCode = areaPojo.getBelongToCode();
            String parentName = areaPojo.getBelongToName();
            String full_name = name;
            if (!name.equals(parentName))
            {
                full_name = parentName + "," + full_name; 
            }
            
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "code", Bytes.toBytes(code));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "name", Bytes.toBytes(name));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "parent_code", Bytes.toBytes(parentCode));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "parent_name", Bytes.toBytes(parentName));
            HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY, "full_name", Bytes.toBytes(full_name));
        }
        
        HBaseTools.insertIntoHBase(HBASE_TABLE_NAME, preparedData);
    }
    
    
    /**
     * 根据已知的地区编码获得子地区信息。
     * @param template  URL模板
     * @param code      地区编码
     * @return
     */
    private static String requestRequest(String template, String code)
    {
        String url = MessageFormat.format(template, code);
        System.out.println("Request URL: " + url);
        
        String apiResult = invokeAPI(url, null);
        System.out.println(apiResult);
        return apiResult;
    }
    
    
    private static String invokeAPI(String url, String proxy)
    {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;
        try
        {
            httpGet.setHeader("Referer", REQUEST_REFERER);
            httpGet.setHeader("User-Agent", REQUEST_UA);
            
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
            
            return EntityUtils.toString(entity, "utf-8");
        }
        catch (IOException e)
        {
            logger.error("<= TONY => Error url: " + url);
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
                    logger.error("<= TONY => Error url: " + url);
                }
            }
        }
        return null;
    }
}

