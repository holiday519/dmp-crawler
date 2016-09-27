package com.pxene.dmp.crawler.social.worker;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.Interval;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.crawler.social.currency.Product;
import com.pxene.dmp.crawler.social.currency.Resource;
import com.pxene.dmp.crawler.social.utils.HBaseTools;

import redis.clients.jedis.Jedis;


public class WXEntityImporter
{
    private static Logger logger = LogManager.getLogger(WXEntityImporter.class.getName());
    
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
    private static final String REDIS_HOST = "115.182.33.163";
    private static final int REDIS_PORT = 7000;
    private static final int REDIS_TIMEOUT = 10000;
    private static final int REDIS_DB = 10;
    private static final String REDIS_SET_KEY = "weixin";
    
    
    private WXSolrIndexBuilder wxSolrIndexBuilder = new WXSolrIndexBuilder();
    
    
    public void doImport(Product product, Resource resource)
    {
        String biz = product.getBiz();
        String mid = product.getMid();
        String idx = product.getIdx();
        String sn = product.getSn();
        String hbaseRowkey = biz + "_" + mid + "_" + idx + "_" + sn;
        logger.debug("==> RowKey: " + hbaseRowkey);
        
        boolean needCrawlerArt = false;
        boolean needCrawlerBiz = false;
        
        // 初始化HBase连接
        Connection connection = resource.getHBaseConnection();
        if (connection == null || connection.isClosed())
        {
            System.err.println("HBase client connection is not ready.");
            System.exit(-1);
        }
        
        // 文章表：
        //      - 确保不会取到重复的biz, mid, idx, sn组合
        if (!isRowKeyExist(connection, "t_prod_weixin_art", hbaseRowkey))
        {
            needCrawlerArt = true;
        }
        
        // 公众号表：
        //      - 确保不会取到重复的biz
        //      - 确保biz作为rowKey未过期
        if (!isRowKeyExist(connection, "t_prod_weixin_biz", biz))
        {
            needCrawlerBiz = true;
        }
        else
        {
            if (!isRowExpired(connection, "t_prod_weixin_biz", biz))
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
        logger.info("==> Target URL: " + url);
        
        // 解析爬取的HTML页面，获得公众号信息+文章信息
        OfficialAccount tmpOfficialAccount = doReatableParse(url);
        tmpOfficialAccount.setMetaData(product);
        
        if (tmpOfficialAccount.getWeixinCode() != null && !"".equals(tmpOfficialAccount.getWeixinCode()))
        {
            if (needCrawlerArt)
            {
                // 插入到HBase文章表(t_weixin_art_prod)
                try
                {
                    insertIntoHBase(HBASE_TABLE_NAME_ART, prepareArtData(hbaseRowkey, tmpOfficialAccount));
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
                    insertIntoHBase(HBASE_TABLE_NAME_BIZ, prepareBizData(biz, tmpOfficialAccount));
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
            return;
        }
        
        // -- 关闭HBase连接 --  \\
        if (connection != null)
        {
            try
            {
                logger.debug("Closing the HBase connection.");
                connection.close();
            }
            catch (IOException e)
            {
                logger.fatal("HBase connection faild, see detail: " + e);
                System.exit(-1);
            }
        }
        
        // 在Solr中构建索引
        /*
        logger.info("开始执行Solr索引构建操作...");
        insertIntoSolr(tmpOfficialAccount, hbaseRowkey, dateStr, jedis);
        */
        
    }
    
    /**
     * 判断Rowkey是否存于在HBase中.
     * @param connection
     * @param tableName
     * @param hbaseRowkey
     * @return
     */
    private boolean isRowKeyExist(Connection connection, String tableName, String hbaseRowkey)
    {
        try
        {
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(hbaseRowkey));
            if (table.exists(get))
            {
                return true;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
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
    private boolean isRowExpired(Connection connection, String tableName, String hbaseRowkey)
    {
        try
        {
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
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
        return false;
    }

    public void redisHset(Jedis jedis, String key, String field, String value)
    {
        jedis.select(14);
        jedis.hset(key, field, value);
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
        
        //HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();  
        //CloseableHttpClient closeableHttpClient = httpClientBuilder.build();
        CloseableHttpClient closeableHttpClient = createSSLClientDefault();
        
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
            //e.printStackTrace();
        }
        catch (NoSuchAlgorithmException e)
        {
            //e.printStackTrace();
        }
        catch (KeyStoreException e)
        {
            //e.printStackTrace();
        }
        
        return HttpClients.createDefault();
    }


    private Map<String, Map<String, Map<String, byte[]>>> insertData(Map<String, Map<String, Map<String, byte[]>>> rowDatas, String rowKey, String familyName, String columnName, byte[] columnVal)
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
    
    
    private Map<String, Map<String, byte[]>> insertData(Map<String, Map<String, byte[]>> familyDatas, String familyName, String columnName, byte[] columnVal)
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
    
    
    private Map<String, byte[]> insertData(Map<String, byte[]> columnDatas, String columnName, byte[] columnVal)
    {
        columnDatas.put(columnName, columnVal);
        return columnDatas;
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
        
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "biz", Bytes.toBytes(oa.getMetaData().getBiz()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "nickname", Bytes.toBytes(oa.getWeixinName()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "profile", Bytes.toBytes(oa.getWeixinDescription()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_ART, "value", Bytes.toBytes(oa.getWeixinCode()));
        
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
        
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "article_title", Bytes.toBytes(oa.getArticleTitle()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "article_date", Bytes.toBytes(oa.getArticleDate()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "article_content", Bytes.toBytes(oa.getArticleContent()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "account_code", Bytes.toBytes(oa.getWeixinCode()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "account_name", Bytes.toBytes(oa.getWeixinName()));
        insertData(preparedData, rowKey, HBASE_COLUMN_FAMILY_BIZ, "account_desc", Bytes.toBytes(oa.getWeixinDescription()));
        
        return preparedData;
    }
    
    
    private void insertIntoHBase(String tblName, Map<String, Map<String, Map<String, byte[]>>> preparedData) throws IOException
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
    
    @SuppressWarnings("unused")
    private void insertIntoSolr(OfficialAccount tmpOfficialAccount, String hbaseRowkey, String dateStr, Jedis jedis)
    {
        // 使用组织好的数据构建Solr索引
        try
        {
            if (wxSolrIndexBuilder.doBuildIndex(tmpOfficialAccount) > 0)
            {
                logger.debug("已成功创建Solr索引.");
            }
        }
        catch (Exception exception)
        {
            redisHset(jedis, "fail_job_solr_index", hbaseRowkey, dateStr);
            exception.printStackTrace();
        }
        
    }
    
    public static void main(String[] args) throws IOException
    {
        String biz = "OTkwMzA0NzAx";
        String mid = "2652181605";
        String idx = "4";
        String sn = "b0a56429b8a2da2e8de66e022883662f";
        
        String hbaseRowkey = biz + "_" + mid + "_" + idx + "_" + sn;
        
        logger.info("==> RowKey: " + hbaseRowkey);
        
        String url = MessageFormat.format(ARITICLE_URL_TEMPLATE, biz, mid, idx, sn);
        logger.info("==> URL: " + url);
        
        url = "https://mp.weixin.qq.com/s?__biz=NzQ3MzYxNDAx&mid=2652344551&idx=3&sn=04ed435967600e78924e20cd29cc02fa&scene=0&key=b28b03434249256b807640f6a1953a6993620b3fd44aa986d2c83e98723d286b25205f39de863ebdc1bd9354ba637fb9&ascene=7&uin=NzA1NzQ3OTIx&devicetype=android-22&version=26030f35&nettype=WIFI&pass_ticket=CFFJg1TRuxHmFioCRktgVIGpeFnFDICSKyxGfyI4aXzP4mjRMN2dw6xARkLJ2W0z";
        System.out.println("zy said : " +url);
        OfficialAccount tmpOfficialAccount = doReatableParse(url);
        if (tmpOfficialAccount.getWeixinCode() != null && !"".equals(tmpOfficialAccount.getWeixinCode()))
        {
            System.out.println(tmpOfficialAccount);
        }
        else
        {
            System.out.println("####################");
        }
        
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