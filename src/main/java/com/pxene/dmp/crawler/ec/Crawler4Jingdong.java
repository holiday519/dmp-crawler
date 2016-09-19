package com.pxene.dmp.crawler.ec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pxene.dmp.common.AjaxClient;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.SimpleWebCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;
import redis.clients.jedis.Jedis;


public class Crawler4Jingdong extends SimpleWebCrawler
{
    private static Logger logger = LogManager.getLogger(Crawler4Jingdong.class.getName());
    
    private static final JsonParser JSON_PARSER = new JsonParser();
    
    /**
     * 需要排除掉的资源型文件的正则规则
     */
    private static final Pattern REGFILTER_EXTENSION = Pattern.compile(".*(\\.(css|js|gif|jp[e]?g|png|mp3|mp4|zip|gz))$");
    
    /**
     * 京东全部页面的正则规则
     */
    private static final Pattern REGFILTER_PAGE_PAGE = Pattern.compile("^http[s]?://\\w*\\.jd\\.com/\\w*$");
    
    /**
     * 京东商品详细页的正则规则
     */
    private static final Pattern REGFILTER_ITEM_PAGE = Pattern.compile("^http[s]?://item\\.jd\\.com/\\d+\\.html$");
    
    /**
     * HBase 表名
     */
    private static final String HBASE_TABLE_NAME = "t_ec_jingdong_baseinfo";
    
    /**
     * HBase RowKey前辍
     */
    private static final String HBASE_ROWKEY_PREFIX = "00040008_";
    
    /**
     * HBase 列簇名称
     */
    private static final String HBASE_COLUMN_FAMILY_PRODUCT = "product_info";
    private static final String HBASE_COLUMN_FAMILY_CLASSIF = "classification_info";
    private static final String HBASE_COLUMN_FAMILY_COMMENT = "comment_info";

    /**
     * Redis配置
     */
    private static final String REDIS_HOST = "192.168.3.178";
    private static final int REDIS_PORT = 7000;
    private static final int REDIS_DB = 10;
    private static final int REDIS_TIMEOUT = 10000;
    private static final String REDIS_SET_KEY = "jingdong";
    
    
    /**
     * 无参构造方法. 用于指定配置文件的路径
     */
    public Crawler4Jingdong()
    {
        super("/" + Crawler4Jingdong.class.getName().replace(".", "/") + ".json");
    }
    
    
    /**
     * 这个方法用来指示给定的URL是否需要被抓取.
     */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url)
    {
        String href = url.getURL().toLowerCase();
        
        if (REGFILTER_EXTENSION.matcher(href).matches())
        {
            return false;
        }
        if (REGFILTER_PAGE_PAGE.matcher(href).matches())
        {
            return true;
        }
        
        return true;
    }
    
    /**
     * 当一个页面被抓取并准备被你的应用处理时，这个方法会被调用.
     */
    @Override
    public void visit(Page page)
    {
        String url = page.getWebURL().getURL();
        
        if (REGFILTER_ITEM_PAGE.matcher(url).matches())
        {
            /*
             * [### For Debug ###] --> Print all HTML page content.
             * System.out.println(((HtmlParseData) page.getParseData()).getHtml());
             */
            logger.debug("线程" + Thread.currentThread().getId() + "发现URL：" + url);
            
            setLogger(logger);
            String randomProxy = null;
            Jedis jedis = null;
            try
            {
                jedis = new Jedis(REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT);
                jedis.select(REDIS_DB);
                randomProxy = jedis.srandmember(REDIS_SET_KEY);
            }
            catch (Exception e)
            {
                logger.debug("Fetch proxy from redis error, see details: " + e);
            }
            finally 
            {
                if (jedis != null)
                {
                    jedis.close();
                }
            }
            
            // 在指定的重试次数内爬取HTML页面
            Document doc = doRepeatableParse(url, randomProxy);
            
            try
            {
                ProductInfoPOJO productInfo = getProductInfo(doc, randomProxy);
                logger.info(productInfo);
                
                if (productInfo != null && productInfo.getProductCode() != null && !"".equals(productInfo.getProductCode()))
                {
                    String rowkey = HBASE_ROWKEY_PREFIX + productInfo.getProductCode();
                    
                    // 声明需要保存至HBase的数据的结果集：
                    Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
                    
                    // 设置商品信息
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_PRODUCT, "product_brand", Bytes.toBytes(productInfo.getProductBrand()));
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_PRODUCT, "product_name", Bytes.toBytes(productInfo.getProductName()));
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_PRODUCT, "product_displayname", Bytes.toBytes(productInfo.getProductDisplayname()));
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_PRODUCT, "product_price", Bytes.toBytes(productInfo.getProductPrice()));
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_PRODUCT, "product_shopname", Bytes.toBytes(productInfo.getProductShop()));
                    
                    // 设置分类信息
                    List<String> productTypeInfo = productInfo.getClassificationInfo();
                    for (int i = 0; i < productTypeInfo.size(); i++)
                    {
                        String columnName = buildTypeColumnName("classification", i+1);
                        insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_CLASSIF, columnName, Bytes.toBytes(productTypeInfo.get(i)));
                    }
                    
                    // 设置评价信息
                    Map<String, Integer> commentInfo = productInfo.getProductCommentInfo();
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_COMMENT, "positive", Bytes.toBytes(commentInfo.get("countPositive")));
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_COMMENT, "moderate", Bytes.toBytes(commentInfo.get("countModerate")));
                    insertData(preparedData, rowkey, HBASE_COLUMN_FAMILY_COMMENT, "negative", Bytes.toBytes(commentInfo.get("countNegative")));
                    
                    
                    // 将组织好的数据插入HBase
                    if (preparedData.size() > 0) 
                    {
                        Table table = HBaseTools.openTable(HBASE_TABLE_NAME);
                        if (table != null) 
                        {
                            HBaseTools.putRowDatas(table, preparedData);
                            HBaseTools.closeTable(table);
                        }
                    }
                }
                else
                {
                    getFailedURLSet().add(url);
                    logger.error("<== TONY ==> 爬取失败，URL：" + url);
                }
            }
            catch (IOException e)
            {
                logger.error(e);
                return;
            }
        }
    }
    
    
    public static void main(String[] args) throws IOException
    {
        testParsePage();
        
        for (int i = 0; i < 11; i++)
        {
            System.out.println(buildTypeColumnName("classification", (i+1)));
        }
    }
    
    public static void testParsePage() throws IOException
    {
        // 自营
        //String url = "http://item.jd.com/3235724.html";
        
        // 商家
//        String url = "http://item.jd.com/3137010.html";
        String url = "http://item.jd.com/10203691212.html";
        
        Connection connection = Jsoup.connect(url);
        Document doc = connection.userAgent("Chrome").get();
        
        ProductInfoPOJO productInfo = getProductInfo(doc, "27.218.119.132:8888");
        System.out.println(productInfo);
    }
    
    private static ProductInfoPOJO getProductInfo(Document doc, String proxy) throws IOException
    {
        ProductInfoPOJO result = new ProductInfoPOJO();
        
        if (doc == null)
        {
            return null;
        }
        
        // 商品分类
        Elements elesProductType = doc.select("div.crumb > :not(.sep)");
        if (elesProductType != null && elesProductType.size() > 0)
        {
            List<String> productTypeInfo = new ArrayList<String>();
            for (Element element : elesProductType)
            {
                productTypeInfo.add(element.text());
            }
            result.setClassificationInfo(productTypeInfo);
        }
        else
        {
            elesProductType = doc.select("div.breadcrumb");
            Element element = null;
            
            String breadCrumb = "";
            for (int i = 0; i < elesProductType.size(); i++)
            {
                element = elesProductType.get(i);
                breadCrumb = breadCrumb + element.text();
            }
            System.out.println(breadCrumb);
            
            if (breadCrumb != null && !"".equals(breadCrumb) && breadCrumb.contains(">"))
            {
                List<String> productTypeInfo = Arrays.asList(breadCrumb.split(" > "));
                
                result.setClassificationInfo(productTypeInfo);
            }
        }
        
        // 商品品牌
        String strProductBrand = "";
        Element eleProductBrand = null;
        Elements elesProductBrand = doc.select("#parameter-brand > li > a");
        if (elesProductBrand != null && elesProductBrand.size() > 0)
        {
            eleProductBrand = elesProductBrand.first();
            strProductBrand = eleProductBrand.text();
        }
        result.setProductBrand(strProductBrand);
        
        // 商品编号、名称、产地、商铺
        Elements elesProductParam = doc.select("div.p-parameter > ul.p-parameter-list > li");
        if (elesProductParam != null && elesProductParam.size() > 0)
        {
            parseProductBaseParam(result, elesProductParam, "title");
        }
        else
        {
            elesProductParam = doc.select("ul.detail-list > li");
            parseProductBaseParam(result, elesProductParam, "title");
        }
        
        // 商品价格
        String productNO = result.getProductCode();
        if (productNO != null && !"".equals(productNO))
        {
            String productPrice = parseProductPriceParam(productNO, proxy);
            
            result.setProductPrice(productPrice);
        }
        
        // 商品显示名称
        Elements elesDisplayname = doc.select("div.sku-name");
        if (elesDisplayname != null && elesDisplayname.size() > 0)
        {
            String productDisplayname = parseElement(elesDisplayname.first(), "text");
            result.setProductDisplayname(productDisplayname);
        }
        else
        {
            elesDisplayname = doc.select("#name h1");
            String productDisplayname = parseElement(elesDisplayname.first(), "text");
            result.setProductDisplayname(productDisplayname);
        }
        
        // 商品评价
        if (productNO != null && !"".equals(productNO))
        {
            parseProductCommentParam(productNO, result);
        }
        
        return result;
    }
    
    
    /**
     * 从HTML元素中，解析出商品介绍，并保存至目标对象中.
     * @param productInfo   用于保存商品信息的对象
     * @param elements      HTML元素对象
     * @param attr          HTML元素的属性名，标准属性或自定义属性，如title或者data-spd
     */
    private static void parseProductBaseParam(ProductInfoPOJO productInfo, Elements elements, String attr)
    {
        if (elements == null || elements.size() <= 0)
        {
            return;
        }
        
        Element element = null;
        String tmpStr = "";
        for (int i = 0; i < elements.size(); i++)
        {
            element = elements.get(i);
            if (element != null)
            {
                if ("text".equals(attr))
                {
                    tmpStr = element.text();
                }
                else
                {
                    tmpStr = element.attr(attr);
                }
                
                
                String text = element.text();
                if (text.contains("商品名称"))
                {
                    productInfo.setProductName(tmpStr);
                }
                else if (text.contains("商品编号")) 
                {
                    productInfo.setProductCode(tmpStr);
                }
                else if (text.contains("商品产地")) 
                {
                    productInfo.setProductOrigin(tmpStr);
                }
                else if (text.contains("店铺")) 
                {
                    productInfo.setProductShop(tmpStr);
                }
            }
        }
        
        // 京东图书需要特殊解析
        List<String> classificationInfo = productInfo.getClassificationInfo();
        if (classificationInfo != null)
        {
            if (classificationInfo.size() > 0 && classificationInfo.get(0) != null && "图书".equals(classificationInfo.get(0)))
            {
                for (Element e : elements)
                {
                    if (e.getElementsContainingOwnText("商品编码：") != null && e.getElementsContainingOwnText("商品编码：").size() > 0)
                    {
                        String code = e.attr("title");
                        productInfo.setProductCode(code);
                    }
                }
            }
        }
    }
    
    /**
     * 调用京东的JSONP格式API，解析出商品价格.
     * @param productNO     商品编号
     * @return
     */
    private static String parseProductPriceParam(String productNO, String proxy)
    {
        String result = "";
        
        Map<String, String> userHeaders = new HashMap<String, String>();
        userHeaders.put("Referer", "http://item.jd.com/" + productNO + ".html");
        userHeaders.put("Cache-Control", "no-cache");
        userHeaders.put("Expires", "0");
        userHeaders.put("X-Requested-With", "XMLHttpRequest");
        
        
//        String userUrl = "http://p.3.cn/prices/mgets?callback=jQuery2410267&type=&pdtk=&pduid=" + getFixLenthString(10) + "&pdpin=&pdbp=0&skuIds=J_" + productNO;
        String userUrl = "http://p.3.cn/prices/get?type=1&skuid=J_" + productNO;
        AjaxClient client = new AjaxClient.Builder(userUrl , userHeaders, proxy).build();
        String resultArrayStr = client.queryJSONArrayStr();
        client.close();
        if (resultArrayStr == null || "".equals(resultArrayStr))
        {
            return "";
        }
        
        
        JsonArray resultArray = null;
//        String callbackParam = StringUtils.regexpExtract(resultArrayStr, "\\w*\\((.*)\\);");
        String callbackParam = resultArrayStr.trim();
        if (callbackParam != null && !"".equals(callbackParam) && callbackParam.contains("["))
        {
            resultArray = JSON_PARSER.parse(callbackParam).getAsJsonArray();
        }
        
        
        if (resultArray != null && resultArray.size() > 0)
        {
            JsonObject tmpObj = resultArray.get(0).getAsJsonObject();
            if (tmpObj != null && tmpObj.has("p"))
            {
                result = tmpObj.get("p").getAsString();
            }
            else
            {
                result = tmpObj.get("m").getAsString();
            }
        }

        return result;
    }
    
    /**
     * 解析出商品的评价数量（好评数、中评数、差评数），并保存至Bean中。
     * <p>
     * 实现的方式为调用京东的JSONP格式接口：<br>
     * 接口返回示例：jQuery2412220({"CommentsCount":[{"SkuId":10153612435,"ProductId":10153612435,"Score1Count":128,"Score2Count":74,"Score3Count":181,"Score4Count":443,"Score5Count":5474,"ShowCount":481,"CommentCount":6300,"AverageScore":5,"GoodCount":5917,"GoodRate":0.94,"GoodRateShow":94,"GoodRateStyle":141,"GeneralCount":255,"GeneralRate":0.04,"GeneralRateShow":4,"GeneralRateStyle":6,"PoorCount":128,"PoorRate":0.02,"PoorRateShow":2,"PoorRateStyle":3}]});
     * </p>
     * @param productNO     商品编号
     * @param pojo          商品对象
     */
    private static void parseProductCommentParam(String productNO, ProductInfoPOJO pojo)
    {
        Map<String, String> userHeaders = new HashMap<String, String>();
        userHeaders.put("Referer", "http://item.jd.com/" + productNO + ".html");
        userHeaders.put("Cache-Control", "no-cache");
        userHeaders.put("Expires", "0");
        userHeaders.put("X-Requested-With", "XMLHttpRequest");
        
        
        String userUrl = "http://club.jd.com/clubservice.aspx?method=GetCommentsCount&referenceIds=" + productNO +"&callback=jQuery2412220&_=" + (new Date().getTime());
        AjaxClient client = new AjaxClient.Builder(userUrl , userHeaders).build();
        String resultArrayStr = client.queryJSONArrayStr();
        client.close();
        
        JsonObject resultObj = null;
        String callbackParam = StringUtils.regexpExtract(resultArrayStr, "\\w*\\((.*)\\);");
        if (callbackParam != null && !"".equals(callbackParam) && callbackParam.contains("["))
        {
            resultObj = JSON_PARSER.parse(callbackParam).getAsJsonObject();
        }
        
        int goodCount = 0;
        int generalCount = 0;
        int poorCount = 0;
        
        if (resultObj != null && resultObj.has("CommentsCount"))
        {
            JsonArray resultArray = resultObj.get("CommentsCount").getAsJsonArray();
            JsonObject tmpObj = resultArray.get(0).getAsJsonObject();
            
            if (tmpObj != null && tmpObj.has("GoodCount"))
            {
                goodCount = tmpObj.get("GoodCount").getAsInt();
            }
            if (tmpObj != null && tmpObj.has("GoodCount"))
            {
                generalCount = tmpObj.get("GeneralCount").getAsInt();
            }
            if (tmpObj != null && tmpObj.has("GoodCount"))
            {
                poorCount = tmpObj.get("PoorCount").getAsInt();
            }
        }
        
        Map<String, Integer> commentInfo = new HashMap<String, Integer>();
        commentInfo.put("countPositive", goodCount);
        commentInfo.put("countModerate", generalCount);
        commentInfo.put("countNegative", poorCount);
        
        pojo.setProductCommentInfo(commentInfo);
    }
    
    /**
     * 解析DOM元素的指定属性，如果元素为null或者属性不存在，则返回空字符串.
     * @param element
     * @return
     */
    private static String parseElement(Element element, String attr)
    {
        String result = "";
        
        if (element != null)
        {
            if (element.hasAttr(attr))
            {
                result = element.attr(attr);
            }
            else if (attr.equals("text"))
            {
                result = element.text();
            }
        }
        
        return result;
    }
    
    /**
     * 构造一个指定的字符串，格式为指定的前缀 + 一个2位的整数。不如2位的序号会在前面补0.
     * <p>
     * 即：当传入的前缀是tony，传入的序号是9，则返回tony09；当传入的序号是99，则返回tony99.
     * </p>
     * @param prefix        前缀字符串
     * @param sequenceNO    当前的序号
     * @return
     */
    private static String buildTypeColumnName(String prefix, int sequenceNO)
    {
        String suffix = "";
        if (sequenceNO < 10)
        {
            suffix = "0" + sequenceNO;
        }
        else 
        {
            suffix = "" + sequenceNO;
        }
        
        return prefix + suffix;
    }
    
    
    /**
     * 返回固定长度的随机数，在前面补0
     * @param strLength
     * @return
     */
    public static String getFixLenthString(int strLength) 
    {
        
        Random rm = new Random();
        
        // 获得随机数
        double pross = (1 + rm.nextDouble()) * Math.pow(10, strLength);

        // 将获得的获得随机数转化为字符串
        String fixLenthString = String.valueOf(pross);

        // 返回固定的长度的随机数
        return fixLenthString.substring(1, strLength + 1);
    }
}

/**
 * 商品实体类.
 * @author ningyu
 */
class ProductInfoPOJO
{
    private String productPrice = "";
    private String productBrand = "";
    private String productDisplayname = "";
    private String productShop = "";
    private String productName = "";
    private String productCode = "";
    private String productOrigin = "";
    private Map<String, Integer> productCommentInfo = new HashMap<String, Integer>();
    private List<String> classificationInfo = new ArrayList<String>();
    
    
    public String getProductPrice()
    {
        return productPrice;
    }
    public void setProductPrice(String productPrice)
    {
        this.productPrice = productPrice;
    }
    public String getProductBrand()
    {
        return productBrand;
    }
    public void setProductBrand(String productBrand)
    {
        this.productBrand = productBrand;
    }
    public String getProductDisplayname()
    {
        return productDisplayname;
    }
    public void setProductDisplayname(String productDisplayname)
    {
        this.productDisplayname = productDisplayname;
    }
    public String getProductShop()
    {
        return productShop;
    }
    public void setProductShop(String productShop)
    {
        this.productShop = productShop;
    }
    public String getProductName()
    {
        return productName;
    }
    public void setProductName(String productName)
    {
        this.productName = productName;
    }
    public String getProductCode()
    {
        return productCode;
    }
    public void setProductCode(String productCode)
    {
        this.productCode = productCode;
    }
    public String getProductOrigin()
    {
        return productOrigin;
    }
    public void setProductOrigin(String productOrigin)
    {
        this.productOrigin = productOrigin;
    }
    public Map<String, Integer> getProductCommentInfo()
    {
        return productCommentInfo;
    }
    public void setProductCommentInfo(Map<String, Integer> productCommentInfo)
    {
        this.productCommentInfo = productCommentInfo;
    }
    public List<String> getClassificationInfo()
    {
        return classificationInfo;
    }
    public void setClassificationInfo(List<String> classificationInfo)
    {
        this.classificationInfo = classificationInfo;
    }
    
    
    public ProductInfoPOJO()
    {
        super();
    }
    public ProductInfoPOJO(String productPrice, String productBrand, String productDisplayname, String productShop, String productName, String productCode, String productOrigin, Map<String, Integer> productCommentInfo, List<String> classificationInfo)
    {
        super();
        this.productPrice = productPrice;
        this.productBrand = productBrand;
        this.productDisplayname = productDisplayname;
        this.productShop = productShop;
        this.productName = productName;
        this.productCode = productCode;
        this.productOrigin = productOrigin;
        this.productCommentInfo = productCommentInfo;
        this.classificationInfo = classificationInfo;
    }
    
    
    @Override
    public String toString()
    {
        return "ProductInfoPOJO [productPrice=" + productPrice + ", productBrand=" + productBrand + ", productDisplayname=" + productDisplayname + ", productShop=" + productShop + ", productName="
                + productName + ", productCode=" + productCode + ", productOrigin=" + productOrigin + ", productCommentInfo=" + productCommentInfo + ", classificationInfo=" + classificationInfo + "]";
    }
}
