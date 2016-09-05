package com.pxene.dmp.crawler.stock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.pxene.dmp.common.AjaxClient;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.IPageCrawler;
import com.pxene.dmp.common.StringUtils;


public class Crawler410jqka implements IPageCrawler
{
    private static Logger logger = LogManager.getLogger(Crawler410jqka.class.getName());
    
    private static final String HBASE_TABLE_NAME = "t_stock_baseinfo";
    
    private static final String HBASE_COLUMN_FAMILY = "stock_info";
    
    private static final String HBASE_ROWKEY_PREFIX = "00150107_";
    
    
    /**
     * 深圳A股主页
     */
    private final static String HOMEPAGE_URL_SZA = "http://q.10jqka.com.cn/stock/fl/sza/#refCountId=qs_fl";
    
    /**
     * 深圳B股主页
     */
    private final static String HOMEPAGE_URL_SZB = "http://q.10jqka.com.cn/stock/fl/szb/#refCountId=qs_fl";
    
    /**
     * 上海A股主页
     */
    private final static String HOMEPAGE_URL_SHA = "http://q.10jqka.com.cn/stock/fl/sha/#refCountId=qs_fl";
    
    /**
     * 上海B股主页
     */
    private final static String HOMEPAGE_URL_SHB = "http://q.10jqka.com.cn/stock/fl/shb/#refCountId=qs_fl";
    
    /**
     * 沪深AB股主页数组
     */
    private final static String[] HOMEPAGE_URL_ARRAY = {HOMEPAGE_URL_SZA, HOMEPAGE_URL_SZB, HOMEPAGE_URL_SHA, HOMEPAGE_URL_SHB};
    
    /**
     * 沪深AB股详细页
     */
    private final String HOMEPAGE_URL_STOCKDETAILS = "http://stockpage.10jqka.com.cn/";
    
    
    @Override
    public void doCrawl(String[] args) throws IOException
    {
        // 声明需要保存至HBase的数据的结果集：
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        for (String HOMEPAGE_URL : HOMEPAGE_URL_ARRAY)
        {
            logger.info("<== TONY ==> 入口 URL: " + HOMEPAGE_URL);
            String stockType = StringUtils.regexpExtract(HOMEPAGE_URL, "/stock/fl/([a-zA-Z]+)/");
            logger.info("股票类型：" + stockType);
            
            Document document = Jsoup.connect(HOMEPAGE_URL).get();
            Element pageInfoElement = document.select("span.page_info").first();
            logger.info(pageInfoElement.html());
            int totalPage = Integer.valueOf(StringUtils.regexpExtract(pageInfoElement.html(), "[0-9]+/([0-9]+)"));
            logger.info("总页数：" + totalPage);
            
            try
            {
                for (int i = 1; i <= totalPage; i++)
                {
                    Map<String, String> userHeaders = new HashMap<String, String>();
                    userHeaders.put("Referer", "http://q.10jqka.com.cn/stock/fl/" + stockType + "/");
                    userHeaders.put("Cache-Control", "no-cache");
                    userHeaders.put("Expires", "0");
                    userHeaders.put("X-Requested-With", "XMLHttpRequest");
                    
                    String userUrl = "http://q.10jqka.com.cn/interface/stock/fl/stockcode/asc/" + i + "/" + stockType + "/quote";
                    logger.info("<== TONY ==> 详细 URL: "  + userUrl);
                    AjaxClient client = new AjaxClient.Builder(userUrl , userHeaders).build();
                    JsonObject result = client.execute();
                    client.close();
                    
                    
                    String stockcode = "";
                    String stockid = "";
                    String stockname = "";
                    String stockplate = "";
                    JsonArray dataArray = result.get("data").getAsJsonArray();
                    for (int j = 0; j < dataArray.size(); j++)
                    {
                        JsonObject tmpObj = dataArray.get(j).getAsJsonObject();
                        
                        stockcode = tmpObj.get("stockcode").getAsString();
                        stockid = tmpObj.get("stockid").getAsString();
                        stockname = tmpObj.get("stockname").getAsString();
                        stockplate = getStockPlate(stockcode);
                        
                        logger.info("rowKey: " + stockcode + ", stockid: " + stockid + ", stockname: " + stockname + ", stockplate: " + stockplate);
                        
                        insertData(preparedData, HBASE_ROWKEY_PREFIX + stockcode, HBASE_COLUMN_FAMILY, "stockid", Bytes.toBytes(stockid));
                        insertData(preparedData, HBASE_ROWKEY_PREFIX + stockcode, HBASE_COLUMN_FAMILY, "stockname", Bytes.toBytes(stockname));
                        insertData(preparedData, HBASE_ROWKEY_PREFIX + stockcode, HBASE_COLUMN_FAMILY, "stockplate", Bytes.toBytes(stockplate));
                    }
                }
                
                
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
            catch (Exception e)
            {
                e.printStackTrace();
            }
            logger.info("===========================================\n");
        }
        
        
        for (Entry<String, Map<String, Map<String, byte[]>>> entry : preparedData.entrySet())
        {
            logger.info(entry.getKey() + " == " + entry.getValue());
        }
        logger.info(preparedData.size());
    }
    
    
    /**
     * 获得股票的所属板块信息，默认为三次重试.
     * @param stockCode
     * @return
     */
    public String getStockPlate(String stockCode)
    {
        String result = "";
        
        int redo = 0;
        while (redo < 3)
        {
            try
            {
                result = fetchStockPlate(stockCode);
                break;
            }
            catch(Exception exception)
            {
                redo++;
                logger.info("正在进行第" + redo + "次重试...");
                continue;
            }
        }
        
        return result;
    }
    
    /**
     * 获得股票和板块信息.
     * <p>
     * 使用股票代码来拼接股票的详情页，如：http://stockpage.10jqka.com.cn/002419/，请求此URL，解析这个界面中的“所属地域”和“涉及概念”两个字段.
     * </p>
     * @param stockCode     股票代码        
     * @return              使用逗号分隔的字符串
     * @throws IOException
     */
    public String fetchStockPlate(String stockCode) throws IOException
    {
        List<String> stockPlates = new ArrayList<String>();
        
        String url = HOMEPAGE_URL_STOCKDETAILS + stockCode;
        Document document = Jsoup.connect(url).get();
            
        // 抓取股票所属的地域板块
        Elements elements = document.select("dl.company_details > dd");
        
        String territoryString = "";
        if (elements != null && elements.size() > 0)
        {
            territoryString = elements.get(0).text() + "板块";
            stockPlates.add(territoryString);
        }
        //logger.info(territoryString);
        
        // 抓取股票所属的概念板块
        String conceptString = "";
        if (elements != null && elements.size() > 1)
        {
            if (elements.get(1).hasAttr("title"))
            {
                conceptString = elements.get(1).attr("title");
                stockPlates.add(conceptString);
            }
        }
        //logger.info(conceptString);
        
        return org.apache.commons.lang.StringUtils.join(stockPlates, ",");
    }
    
    
    protected Map<String, Map<String, Map<String, byte[]>>> insertData(Map<String, Map<String, Map<String, byte[]>>> rowDatas, String rowKey, String familyName, String columnName, byte[] columnVal)
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
    
    protected Map<String, Map<String, byte[]>> insertData(Map<String, Map<String, byte[]>> familyDatas, String familyName, String columnName, byte[] columnVal)
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
    
    protected Map<String, byte[]> insertData(Map<String, byte[]> columnDatas, String columnName, byte[] columnVal)
    {
        columnDatas.put(columnName, columnVal);
        return columnDatas;
    }
    
    
    public static void main(String[] args) throws IOException
    {
        Crawler410jqka crawler = new Crawler410jqka();
        crawler.doCrawl(null);
    }
    
}