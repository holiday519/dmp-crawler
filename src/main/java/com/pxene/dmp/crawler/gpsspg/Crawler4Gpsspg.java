package com.pxene.dmp.crawler.gpsspg;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Table;
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

import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.social.utils.HBaseTools;

public class Crawler4Gpsspg implements IPageCrawler
{
    private static Logger logger = LogManager.getLogger(Crawler4Gpsspg.class.getName());
    
    /**
     * URL模板
     */
    private static final String REQUEST_URL_TEMPLATE = "http://api.gpsspg.com/bss/?oid=159&bs={0}&hex={1}&type={2}&to=3&output=jsonp&callback=jQuery110207956256142351776_{3}&_={4}";
    
    /**
     * BS参数的分隔符
     */
    private static final String BS_STRING_SEPERATOR = ",";
    
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
    
    /**
     * HBase表名
     */
    private static final String HBASE_TABLE_NAME = "t_bs_telecom";
    
    /**
     * HBase列簇名称
     */
    private static final String HBASE_COLUMN_FAMILY_NAME = "info";

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
     * 默认的国家代码
     */
    private static final String DEFAULT_MMC = "460";
    
    
    public static void main(String[] args) throws InterruptedException
    {
        Crawler4Gpsspg crawler = new Crawler4Gpsspg();
        /*
        String url = getURL4GSMOrUTMSOrLTE(Enum4ENC.CHINA_UNICOM, "41032", "5782479", "10");
        
        while (true)
        {
            System.out.println(url);
            String crawlerResult = crawler.invokeAPI(url, "101.69.178.145:7777");
            
            System.out.println(crawlerResult);
            String jsonStr = "";
            JSONObject jsonObject = null;
            if (crawlerResult != null && !"".equals(crawlerResult))
            {
                jsonStr = StringUtils.regexpExtract(crawlerResult, "jQuery\\d+.*\\((\\{.*\\})+\\)");
                jsonObject = new JSONObject(jsonStr);
            }
            System.out.println(jsonObject);
            
            System.out.println(crawler.str2BsidMetaData(crawlerResult));
            
            Thread.sleep(112000);
        }
        */
        BSPojo bs = crawler.getBSByTelecomCMDA("36000001E771");
        System.out.println("BS-Pojo: " + bs);
    }
    
    
    @Override
    public void doCrawl(String[] args) throws Exception
    {
        loadAndSave("1=1");
    }
    
    
    /**
     * 从Hive表中取得imsi, bsid, datetime三个参数，以此构建查询基站API的URL，爬取这个URL，解析其内容，保存至HBase中。
     * @param dataStr           需要从Hive中查找的规定日期
     * @return
     * @throws IOException
     * @throws SQLException
     */
    public void loadAndSave(String condStr) throws IOException, SQLException
    {
        Connection con = null;
        try
        {
            Class.forName(driverName);
            
            con = DriverManager.getConnection(HIVE_URL, HIVE_USERNAME, "");
            
            Statement stmt = con.createStatement();
            
            logger.info("正在查询Hive表：‘ee_bsid_v00’，请稍候......");
            
            String sql = "select distinct(bsid) from ee_bsid_v00 where imsi != '' and bsid != '' and datetime != '' and " + condStr;
            logger.info("查询SQL：" + sql);
            ResultSet res = stmt.executeQuery(sql);
            
            logger.debug("Scan Result:");
            logger.debug("imsi \t bsid \t datetime");
            
            String bsid = "";
            
            int i = 0;
            while (res.next()) 
            {
                bsid = res.getString(1);
                
                BSPojo bs = getBSByTelecomCMDA(bsid);
                if (bs != null && !"".equals(bsid))
                {
                    insertIntoHBase(HBASE_TABLE_NAME, prepareBSData(bsid, bs));
                    i++;
                }
                else
                {
                    logger.error("<= TONY => BSID parse error: [bsid=" + bsid + "].");
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
    
    /**
     * 根据BSID获得基站地理位置。
     * @param bsid
     * @return
     */
    private BSPojo getBSByTelecomCMDA(String bsid)
    {
        if (bsid != null && !"".equals(bsid) && bsid.length() == 12)
        {
            String sid = bsid.substring(0, 4);
            String nid = bsid.substring(4, 8);
            String bid = bsid.substring(8, 12);
            String url = getURL4CDMA(sid, nid, bid, "16");
            logger.info("Target bsid: " + bsid + ". ---- Target API URL: " + url);
            
            String reqResult = invokeAPI(url, null);
            logger.info("API Request result: " + reqResult);
            
            return str2BsidMetaData(reqResult);
        }
        return null;
    }
    

    
    /**
     * 构造需要插入到HBase中数据的集合。
     * @param rowKey    HBase表的Rowkey
     * @param bs        保存基站信息的实体
     * @return
     */
    private static Map<String, Map<String, Map<String, byte[]>>> prepareBSData(String rowKey, BSPojo bs)
    {
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "id", Bytes.toBytes(bs.getId()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "lat", Bytes.toBytes(bs.getLat()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "lng", Bytes.toBytes(bs.getLng()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "radius", Bytes.toBytes(bs.getRadius()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "address", Bytes.toBytes(bs.getAddress()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "roads", Bytes.toBytes(bs.getRoads()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "rid", Bytes.toBytes(bs.getRid()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_NAME, "rids", Bytes.toBytes(bs.getRids()));
        
        return preparedData;
    }
    
    /**
     * 向HBase中插入构造好的数据。
     * @param tblName       HBase的表名
     * @param preparedData  准备好的数据
     * @throws IOException
     */
    private static void insertIntoHBase(String tblName, Map<String, Map<String, Map<String, byte[]>>> preparedData) throws IOException
    {
        // 将组织好的数据插入HBase
        if (preparedData.size() > 0) 
        {
            Table table = HBaseTools.openTable(tblName);
            if (table != null) 
            {
                HBaseTools.putRowDatas(table, preparedData);
                HBaseTools.closeTable(table);
            }
        }
    }
    
    
    /**
     * 构造GSM/UTMS/LTE请求URL。
     * @param enc   网络制式
     * @param lac   LAC
     * @param cid   CellId
     * @param hex   进制
     * @return
     */
    @SuppressWarnings("unused")
    private static String getURL4GSMOrUTMSOrLTE(Enum4ENC enc, String lac, String cid, String hex)
    {
        String[] array = {DEFAULT_MMC, enc.getCode(), lac, cid};
        String bsString = org.apache.commons.lang3.StringUtils.join(array, BS_STRING_SEPERATOR);
        return getReqUrl(bsString, hex, Enum4Type.GSM_UMTS_LTE);
    }
    
    /**
     * 构造CDMA请求URL。
     * @param sid   SID
     * @param nid   NID
     * @param bid   BID
     * @param hex   进制
     * @return
     */
    private static String getURL4CDMA(String sid, String nid, String bid, String hex)
    {
        String[] array = {DEFAULT_MMC, sid, nid, bid};
        String bsString = org.apache.commons.lang3.StringUtils.join(array, BS_STRING_SEPERATOR);
        return getReqUrl(bsString, hex, Enum4Type.CDMA);
    }
    
    /**
     * 构造请求URL。
     * @param type      网络制式： GSM/UMTS/LTE 或 CDMA
     * @param bs        如果是GSM/UMTS/LTE则格式为： “国家代码,网络制式,LAC,CellId”，如果是CDMA则格式为：“国家代码,SID,NID,BID”
     * @param hex       参数的进制：10或16
     * @param mmc       国家代码
     * @see com.pxene.dmp.crawler.gpsspg.Enum4ENC
     * @return
     */
    private static String getReqUrl(String bs, String hex, Enum4Type type)
    {
        String beginDateString = String.valueOf(new Date().getTime());
        String endDateString = String.valueOf(new Date().getTime() + 600);
        
        return MessageFormat.format(REQUEST_URL_TEMPLATE, bs, hex, type.getType(), beginDateString, endDateString);
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
    
    /**
     * 将API调用结果的JSON字符串转换成JavaBean。
     * @param source    原始的API返回值
     * @return
     */
    private BSPojo str2BsidMetaData(String source)
    {
        if (source == null || "".equals(source))
        {
            return null;
        }
        
        BSPojo result = null;
        
        JSONObject jsonObject = null;
        String jsonStr = StringUtils.regexpExtract(source, "jQuery\\d+.*\\((\\{.*\\})+\\)");
        
        if (jsonStr != null && !"".equals(jsonStr))
        {
            jsonObject = new JSONObject(jsonStr);
            
            String id = null;
            String lat = null;
            String lng = null;
            String radius = null;
            String address = null;
            String roads = null;
            String rid = null;
            String rids = null;
            
            if (jsonObject.has("status") && (200 == jsonObject.getInt("status")))
            {
                JSONObject resultObj = jsonObject.getJSONArray("result").getJSONObject(0);
                id = resultObj.getString("id");
                lat = resultObj.getString("lat");
                lng = resultObj.getString("lng");
                radius = resultObj.getString("radius");
                address = resultObj.getString("address");
                roads = resultObj.getString("roads");
                rid = resultObj.getString("rid");
                rids = resultObj.getString("rids");
                
                result = new BSPojo(id, lat, lng, radius, address, roads, rid, rids);
            }
        }
        
        return result;
    }

    private static Map<String, Map<String, Map<String, byte[]>>> insertData(Map<String, Map<String, Map<String, byte[]>>> rowDatas, String rowKey, String familyName, String columnName, byte[] columnVal)
    {
        if (rowDatas.containsKey(rowKey))
        {
            rowDatas.put(rowKey, insertData(rowDatas.get(rowKey), familyName, columnName, columnVal));
        }
        else
        {
            rowDatas.put(rowKey, insertData(new HashMap<String, Map<String, byte[]>>(), familyName, columnName, columnVal));
        }
        return rowDatas;
    }
    
    
    private static Map<String, Map<String, byte[]>> insertData(Map<String, Map<String, byte[]>> familyDatas, String familyName, String columnName, byte[] columnVal)
    {
        if (familyDatas.containsKey(familyName))
        {
            familyDatas.put(familyName, insertData(familyDatas.get(familyName), columnName, columnVal));
        }
        else
        {
            familyDatas.put(familyName, insertData(new HashMap<String, byte[]>(), columnName, columnVal));
        }
        return familyDatas;
    }
    
    
    private static Map<String, byte[]> insertData(Map<String, byte[]> columnDatas, String columnName, byte[] columnVal)
    {
        columnDatas.put(columnName, columnVal);
        return columnDatas;
    }
    
}
