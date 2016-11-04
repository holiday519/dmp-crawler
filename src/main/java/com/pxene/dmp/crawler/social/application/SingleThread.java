package com.pxene.dmp.crawler.social.application;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.Interval;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.crawler.social.currency.Product;

import redis.clients.jedis.Jedis;

public class SingleThread implements IPageCrawler
{
    private static Logger logger = LogManager.getLogger(SingleThread.class.getName());
    
    /**
     * Hive JDBC驱动
     */
    private static String HIVE_DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";
    
    /**
     * Hive 连接URL
     */
    private static final String HIVE_URL = "jdbc:hive2://dmp01:10000/basetables";
    
    /**
     * Hive 用户名
     */
    private static final String HIVE_USERNAME = "ningyu";
    
    /**
     * 微信文章URL模板
     */
    private final static String ARITICLE_URL_TEMPLATE = "https://mp.weixin.qq.com/s?__biz={0}&mid={1}&idx={2}&sn={3}&scene=0&key=b28b03434249256b807640f6a1953a6993620b3fd44aa986d2c83e98723d286b25205f39de863ebdc1bd9354ba637fb9&ascene=7&uin=NzA1NzQ3OTIx&devicetype=android-22&version=26030f35&nettype=WIFI&pass_ticket=CFFJg1TRuxHmFioCRktgVIGpeFnFDICSKyxGfyI4aXzP4mjRMN2dw6xARkLJ2W0z";
    
    /**
     * 默认的重试次数
     */
    private static final int REPEAT_COUNT = 3;
    
    /**
     * HBase表名
     */
    private static final String HBASE_TABLE_NAME_BIZ = "t_prod_weixin_biz";
    private static final String HBASE_TABLE_NAME_ART = "t_prod_weixin_art";
    
    /**
     * HBase列簇名称
     */
    private static final String HBASE_COLUMN_FAMILY_BIZ = "info";
    private static final String HBASE_COLUMN_FAMILY_ART = "info";

    /**
     * HBase行有效期，即，超过此值后的旧值需要进行更新
     */
    private static final int DEFAULT_EXPIRE_MONTH = 3;
    
    /**
     * Redis配置
     */
    private static final String REDIS_HOST = "dmp08";
    private static final int REDIS_PORT = 7000;
    private static final int REDIS_TIMEOUT = 10000;
    private static final int REDIS_DB = 10;
    private static final String REDIS_SET_KEY = "weixin";
    

    @Override
    public void doCrawl(String[] args) throws Exception
    {
        if (args == null || args.length != 2)
        {
            logger.fatal("参数异常");
            System.exit(-1);
        }
        
        String dateStr = args[0];
        String partitionSource = args[1];
        logger.info("Receive cmd params, dataStr=" + dateStr + ", partitionSource=" + partitionSource);
        
        loadAndSave(dateStr, partitionSource);
    }
    
    public void loadAndSave(String dateStr, String partitionSource) throws SQLException
    {
        Connection con = null;
        try
        {
            Class.forName(HIVE_DRIVERNAME);
            
            con = DriverManager.getConnection(HIVE_URL, HIVE_USERNAME, "");
            
            Statement stmt = con.createStatement();
            
            logger.info("正在查询Hive表：‘weixin’，请稍候......");
            
            String sql = "select biz,mid,idx,sn from weixin where biz != '' and mid != '' and idx != '' and sn != '' and data_time LIKE '" + dateStr +"%' and partition_source = '" + partitionSource + "' group by biz, mid, idx, sn";
            logger.info("查询SQL：" + sql);
            ResultSet res = stmt.executeQuery(sql);
            
            logger.info("Scan Result:");
            logger.info("biz \t mid \t idx \t sn ");
            
            String biz = "";
            String mid = "";
            String idx = "";
            String sn = "";
            
            if (biz.toUpperCase().contains("%3D"))
            {
                biz = URLDecoder.decode(biz, "utf-8");
            }
            
            int i = 0;
            while (res.next()) 
            {
                biz = res.getString(1);
                mid = res.getString(2);
                idx = res.getString(3);
                sn = res.getString(4);
                
                if (biz.toUpperCase().contains("%3D"))
                {
                    biz = URLDecoder.decode(biz, "utf-8");
                }
                
                String hbaseRowkey = biz + "_" + mid + "_" + idx + "_" + sn;
                logger.debug("RowKey: " + hbaseRowkey);
                
                boolean needCrawlerArt = false;
                boolean needCrawlerBiz = false;
                
                // 文章表：
                //      - 确保不会取到重复的biz, mid, idx, sn组合
                if (!isRowKeyExist("t_prod_weixin_art", hbaseRowkey))
                {
                    needCrawlerArt = true;
                }
                
                // 公众号表：
                //      - 确保不会取到重复的biz
                //      - 确保biz作为rowKey未过期
                if (!isRowKeyExist("t_prod_weixin_biz", biz))
                {
                    needCrawlerBiz = true;
                }
                else
                {
                    if (!isRowExpired("t_prod_weixin_biz", biz))
                    {
                        needCrawlerBiz = true;
                    }
                }
                
                // 如果待爬取的内容同时在文章表和公众号都已存在且未过期，则无必要再去请求和解析
                if (!needCrawlerArt && !needCrawlerBiz)
                {
                    return;
                }
                
                // 使用模板构造需要爬取的公众号文章URL
                String url = MessageFormat.format(ARITICLE_URL_TEMPLATE, biz, mid, idx, sn);
                logger.info("Target URL: " + url);
                
                // 解析爬取的HTML页面，获得公众号信息+文章信息
                OfficialAccount tmpOfficialAccount = doReatableParse(url);
                tmpOfficialAccount.setMetaData(new Product(biz, mid, idx, sn));
                
                if (tmpOfficialAccount.getWeixinName() != null && !"".equals(tmpOfficialAccount.getWeixinName()))
                {
                    if (needCrawlerArt)
                    {
                        // 插入到HBase文章表(t_weixin_art_prod)
                        try
                        {
                            HBaseTools.insertIntoHBase(HBASE_TABLE_NAME_ART, prepareArtData(hbaseRowkey, tmpOfficialAccount));
                        }
                        catch (Exception exception)
                        {
                            logger.error("<== TONY ==> 插入t_weixin_art_prod失败，URL：" + url);
                            exception.printStackTrace();
                        }
                    }
                    
                    if (needCrawlerBiz)
                    {
                        // 插入到HBase公众号表(t_weixin_biz_prod)
                        try
                        {
                            HBaseTools.insertIntoHBase(HBASE_TABLE_NAME_BIZ, prepareBizData(biz, tmpOfficialAccount));
                        }
                        catch (Exception exception)
                        {
                            logger.error("<== TONY ==> 插入t_weixin_biz_prod失败，URL：" + url);
                            exception.printStackTrace();
                        }
                    }
                }
                else
                {
                    logger.error("<== TONY ==> 微信号不合法，URL：" + url + " <== TONY ==>  tmpOfficialAccount=" + tmpOfficialAccount);
                }
                
                i++; 
            }
            System.out.println("<= TONY => Total item fetch from Hive: " + i);
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
     * 判断Rowkey是否存于在HBase中.
     * @param connection
     * @param tableName
     * @param hbaseRowkey
     * @return
     */
    private boolean isRowKeyExist(String tableName, String hbaseRowkey)
    {
        Table table = null;
        try
        {
            table = HBaseTools.openTable(tableName);
            Get get = new Get(Bytes.toBytes(hbaseRowkey));
            if (table != null && table.exists(get)) 
            {
                return true;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally 
        {
            HBaseTools.closeTable(table);
        }
        return false;
    }
    
    
    /**
     * 判断指定Rowkey的行是否已经过旧，需要更新。
     * @param connection
     * @param tableName
     * @param hbaseRowkey
     * @return
     */
    private boolean isRowExpired(String tableName, String hbaseRowkey)
    {
        Table table = null;
        try
        {
            table = HBaseTools.openTable(tableName);
            Get get = new Get(Bytes.toBytes(hbaseRowkey));
            get.getTimeRange();
            
            Result result = table.get(get);
            Cell[] rawCells = result.rawCells();
            if (rawCells != null && rawCells.length > 0)
            {
                long rawTime = rawCells[0].getTimestamp();
                long nowTime = new Date().getTime();
                
                Interval interval = new Interval(rawTime, nowTime);
                int months = interval.toPeriod().getMonths();
                if (months >= DEFAULT_EXPIRE_MONTH)
                {
                    return true;
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally 
        {
            HBaseTools.closeTable(table);
        }
        return false;
    }
    
    /**
     * 构造需要插入到公众号HBase中数据的集合.
     * @param preparedData  保存结合的集合
     * @param oa            公众号实体对象
     * @return 
     */
    private Map<String, Map<String, Map<String, byte[]>>> prepareBizData(String rowKey, OfficialAccount oa)
    {
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "biz", Bytes.toBytes(oa.getMetaData().getBiz()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "nickname", Bytes.toBytes(oa.getWeixinName()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "profile", Bytes.toBytes(oa.getWeixinDescription()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "value", Bytes.toBytes(oa.getWeixinCode()));
        
        return preparedData;
    }
    
    /**
     * 构造需要插入到文章HBase中数据的集合.
     * @param preparedData  保存结合的集合
     * @param oa            公众号实体对象
     * @return 
     */
    private Map<String, Map<String, Map<String, byte[]>>> prepareArtData(String rowKey, OfficialAccount oa)
    {
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "article_title", Bytes.toBytes(oa.getArticleTitle()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "article_date", Bytes.toBytes(oa.getArticleDate()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "article_content", Bytes.toBytes(oa.getArticleContent()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "account_code", Bytes.toBytes(oa.getWeixinCode()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "account_name", Bytes.toBytes(oa.getWeixinName()));
        HBaseTools.insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "account_desc", Bytes.toBytes(oa.getWeixinDescription()));
        
        return preparedData;
    }
    
    public static void main(String[] args)
    {
        String url = "https://mp.weixin.qq.com/s?__biz=MTA1NTc0MjE0MA==&mid=2652197182&idx=3&sn=fe3b34621621893e4fdfb5aa4b08e52b&scene=0&key=b28b03434249256b807640f6a1953a6993620b3fd44aa986d2c83e98723d286b25205f39de863ebdc1bd9354ba637fb9&ascene=7&uin=NzA1NzQ3OTIx&devicetype=android-22&version=26030f35&nettype=WIFI&pass_ticket=CFFJg1TRuxHmFioCRktgVIGpeFnFDICSKyxGfyI4aXzP4mjRMN2dw6xARkLJ2W0z";
        OfficialAccount oa = doReatableParse(url);
        System.out.println(oa);
    }
    
    /**
     * 在指定的重试次数内尝试解析指定的公众号文章内容.
     * @param url               公众号文章URL
     * @param jedisPool         Redis连接池，取出Redis中保存的代理IP
     * @return                  封装有标题、时间、内容等信息的对象
     * @throws IOException
     */
    public static OfficialAccount doReatableParse(String url)
    {
        String articleTitle = "";       // 标题
        String articleDate = "";        // 时间
        String articleContent = "";     // 文章内容
        String weixinName = "";         // 公众号名称
        String weixinCode = "";         // 公众号号码
        String weixinDescription = "";  // 公众号描述
        
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT);
        
        int redo = 0;
        while (redo < REPEAT_COUNT)
        {
            try
            {
                String html = getHtmlStr(url, jedis);
                if (html == null || "".equals(html))
                {
                    return null;
                }
                
                Document doc = Jsoup.parse(html);
                
                articleTitle = getDOMTextBySelector(doc, "#activity-name");
                articleDate = getDOMTextBySelector(doc, "#post-date");
                articleContent = getDOMTextBySelector(doc, "#js_content");
                weixinName = getDOMTextBySelector(doc, "#js_profile_qrcode strong.profile_nickname");
                weixinCode = getDOMTextBySelector(doc, "#js_profile_qrcode span.profile_meta_value");
                Elements profiles = doc.select("#js_profile_qrcode span.profile_meta_value");
                if (profiles.size() > 1)
                {
                    weixinDescription = profiles.get(1).text();
                }
                
                logger.debug("\n=====================\n微信公众号：" + weixinName + "(" + weixinCode + ") ---> " + weixinDescription + "\n文章标题：" + articleTitle + "\n发布时间：" + articleDate + "\n文章内容：" + articleContent);
                
                break;
            }
            catch (Exception exception)
            {
                redo++;
                logger.debug(exception);
                logger.debug("正进行第" + redo + "次重试，目标URL：" + url);
                continue;
            }
        }
        
        if (redo >= REPEAT_COUNT)
        {
            logger.error("已进行" + REPEAT_COUNT + "次重试，均告失败，放弃重试，失败URL：" + url);
        }
        
        jedis.close();
        
        return new OfficialAccount(articleTitle, articleDate, articleContent, weixinName, weixinCode, weixinDescription);
    }
    
    private static String getHtmlStr(String url, Jedis jedis)
    {
        String result = null;
        
        String proxyHost = null;
        int proxyPort = 0;
        
        try
        {
            jedis.select(REDIS_DB);
            String randomProxy = jedis.srandmember(REDIS_SET_KEY);
            logger.debug("Redis proxy host " + randomProxy);
            
            if (randomProxy != null && "".equals(randomProxy) && randomProxy.contains(":"))
            {
                String[] tmp = randomProxy.split(":");
                if (tmp.length == 1)
                {
                    proxyHost = tmp[0];
                }
                if (tmp.length == 2)
                {
                    proxyPort = Integer.valueOf(tmp[1]);
                }
            }
            
            // 如果使用代理访问目标URL成功，则直接返回；否则不用代理再尝试一次.
            result = executeHTTPRequest(url, proxyHost, proxyPort);
            if (result == null || "".equals(result))
            {
                logger.debug("Visit target url: '" + url + "' without proxy.");
                result = executeHTTPRequest(url);
            }
        }
        catch (Exception exception)
        {
            logger.info("Visit target url: '" + url + "' without proxy.");
            result = executeHTTPRequest(url);
        }
        
        return result;
    }
    
    /**
     * 从DOM文档中选出指定CSS选择器选出元素的文本内容.
     * @param doc       页面DOM文档
     * @param selector  选择器字符串
     * @return          指定的元素的Text内容
     */
    private static String getDOMTextBySelector(Document doc, String selector)
    {
        String result = "";
        
        Elements tmpElements = null;
        Element tmpElement = null;
        
        tmpElements = doc.select(selector);
        if (tmpElements != null && tmpElements.size() > 0)
        {
            tmpElement = tmpElements.first();
            result = tmpElement.text().trim();
            if (result.endsWith(":") || result.endsWith("："))
            {
                result = result.substring(0, result.length() - 1);
            }
        }
        
        return result;
    }
    
    
    /**
     * 向指定的URL发送请求，并返回HTML的字符串表示.
     * @param url   需要访问的URL
     * @return
     */
    private static String executeHTTPRequest(String url)
    {
        return executeHTTPRequest(url, null, 0);
    }
    
    /**
     * 通过代理服务器向指定的URL发送请求，并返回HTML的字符串表示.
     * @param url       需要访问的URL
     * @param proxyHost 代理服务器IP
     * @param proxyPort 代理服务器端口
     * @return
     */
    private static String executeHTTPRequest(String url, String proxyHost, int proxyPort)
    {
        String html = "";
        
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();  
        CloseableHttpClient closeableHttpClient = httpClientBuilder.build();
        
        HttpGet httpGet = new HttpGet(url);
        if (proxyHost != null && !"".equals(proxyHost) && proxyPort != 0)
        {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort); 
            RequestConfig config = RequestConfig.custom().setProxy(proxy).setSocketTimeout(2000).setConnectTimeout(2000).build();
            httpGet.setConfig(config);
        }
        
        CloseableHttpResponse response = null;
        try
        {
            response = closeableHttpClient.execute(httpGet);
            html = EntityUtils.toString(response.getEntity(), "UTF-8");
            
        }
        catch (Exception e)
        {
            //e.printStackTrace();
        }
        finally 
        {
            try
            {
                if (response != null)
                {
                    response.close();
                }
                if (httpGet != null)
                {
                    httpGet.releaseConnection();
                }
                if (closeableHttpClient != null)
                {
                    closeableHttpClient.close();
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        
        return html;
    }
}


/**
 * 公众号文章实体类.
 * @author ningyu
 */
class OfficialAccount
{
    private Product metaData;
    private String articleTitle;
    private String articleDate;
    private String articleContent;
    private String weixinName;
    private String weixinCode;
    private String weixinDescription;
    
    
    public Product getMetaData()
    {
        return metaData;
    }
    public void setMetaData(Product metaData)
    {
        this.metaData = metaData;
    }
    public String getArticleTitle()
    {
        return articleTitle;
    }
    public void setArticleTitle(String articleTitle)
    {
        this.articleTitle = articleTitle;
    }
    public String getArticleDate()
    {
        return articleDate;
    }
    public void setArticleDate(String articleDate)
    {
        this.articleDate = articleDate;
    }
    public String getArticleContent()
    {
        return articleContent;
    }
    public void setArticleContent(String articleContent)
    {
        this.articleContent = articleContent;
    }
    public String getWeixinName()
    {
        return weixinName;
    }
    public void setWeixinName(String weixinName)
    {
        this.weixinName = weixinName;
    }
    public String getWeixinCode()
    {
        return weixinCode;
    }
    public void setWeixinCode(String weixinCode)
    {
        this.weixinCode = weixinCode;
    }
    public String getWeixinDescription()
    {
        return weixinDescription;
    }
    public void setWeixinDescription(String weixinDescription)
    {
        this.weixinDescription = weixinDescription;
    }
    
    
    public OfficialAccount()
    {
        super();
    }
    public OfficialAccount(String articleTitle, String articleDate, String articleContent, String weixinName, String weixinCode, String weixinDescription)
    {
        super();
        this.articleTitle = articleTitle;
        this.articleDate = articleDate;
        this.articleContent = articleContent;
        this.weixinName = weixinName;
        this.weixinCode = weixinCode;
        this.weixinDescription = weixinDescription;
    }
    
    
    @Override
    public String toString()
    {
        return "OfficialAccount [metaData=" + metaData + ", articleTitle=" + articleTitle + ", articleDate=" + articleDate + ", articleContent=" + articleContent + ", weixinName=" + weixinName + ", weixinCode=" + weixinCode + ", weixinDescription=" + weixinDescription + "]";
    }
}