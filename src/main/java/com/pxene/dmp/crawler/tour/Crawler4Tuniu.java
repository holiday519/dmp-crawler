package com.pxene.dmp.crawler.tour;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;


public class Crawler4Tuniu extends BaseCrawler
{
    private static Logger logger = LogManager.getLogger(Crawler4Tuniu.class.getName());
    
    
    private final static Pattern REGFILTER_EXTENSION = Pattern.compile(".*(\\.(css|js|gif|jp[e]?g|png|mp3|mp4|zip|gz))$");
    
    private final static Pattern REGFILTER_TOURS_PAGE = Pattern.compile("^http[s]?://\\w*\\.tuniu\\.com/tours/[0-9]{9}$");
    
    private final static Pattern REGFILTER_DRIVE_PAGE = Pattern.compile("^http[s]?://\\w*\\.tuniu\\.com/drive/[0-9]{9}$");
    
    /**
     * 表名
     */
    private static final String HBASE_TOUR_TABLE_NAME = "t_tour_baseinfo";
    
    /**
     * RowKey前辍
     */
    private static final String HBASE_ROWKEY_PREFIX = "00100032_";
    
    /**
     * 列簇名称
     */
    private static final String HBASE_TOUR_COLUMN_FAMILY = "route_info";
    
    
    /**
     * 无参构造方法. 用于指定配置文件的路径
     */
    public Crawler4Tuniu()
    {
        super("/" + Crawler4Tuniu.class.getName().replace(".", "/") + ".json");
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
        
        if (REGFILTER_TOURS_PAGE.matcher(href).matches())
        {
            return true;
        }
        
        if (REGFILTER_DRIVE_PAGE.matcher(href).matches())
        {
            return true;
        }
        
        return false;
    }
    
    /**
     * 当一个页面被抓取并准备被你的应用处理时，这个方法会被调用.
     */
    @Override
    public void visit(Page page)
    {
        String url = page.getWebURL().getURL();
        
        /*
         * For Debug -> showBasicPageinfo(url, page);
         */
        
        if (REGFILTER_TOURS_PAGE.matcher(url).matches())
        {
            //getToursInfo(((HtmlParseData) page.getParseData()).getHtml());
            getToursInfo(url);
        }
    }
    
    @Override
    protected void onContentFetchError(WebURL webUrl) 
    {
        if (proxyConf.isEnable()) 
        {
            String[] params = proxyConf.randomIp().split(":");
            System.getProperties().setProperty("proxySet", "true");
            System.getProperties().setProperty("http.proxyHost", params[0]);
            System.getProperties().setProperty("http.proxyPort", params[1]);
        }
    }
    
    
    
    private void getToursInfo(String url)
    {
        try
        {
            logger.info("网页地址：" + url);
            Document doc = Jsoup.connect(url).get();
            if (doc == null)
            {
                return;
            }
            
            // 声明需要保存至HBase的数据的结果集：
            Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
            
            String toursNO = "";
            String toursName = "";
            String toursType = "";
            
            toursNO = StringUtils.regexpExtract(url, "http[s]?://.*\\.tuniu\\.com/tours/(\\d+)");
            
            if (url.contains("temai.tuniu"))    // 途牛特卖需要单独解析
            {
                toursType = "特卖";

                String source = getDOMTextBySelector(doc, ".product_detail > h2");
                
                if (!source.equals(""))
                {
                    toursNO = StringUtils.regexpExtract(source, "（.*：(\\d*)）");
                    toursName = StringUtils.regexpExtract(source, "(.*)（.*）");
                }
                else
                {
                    toursNO = StringUtils.regexpExtract(url, "http[s]?://.*\\.tuniu\\.com/tours/(\\d+)");
                    toursName = getDOMTextBySelector(doc, "#con_top > div.title.clearfix > div.title_l > h1");
                }
            }
            else
            {
                if (doc.select("#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.tours-sub-info.clearfix > div.ser_sm.fl > span.c_f80").size() > 0)
                {
                    toursNO = getDOMTextBySelector(doc, "#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.tours-sub-info.clearfix > div.ser_sm.fl > span.c_f80");
                    if (toursNO.contains("编号"))
                    {
                        toursNO = toursNO.replace("编号", "");
                    }
                    
                    toursName = getDOMTextBySelector(doc, "#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.top_tit > h1");
                    
                    toursType = getDOMClassBySelector(doc, "#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.tours-sub-info.clearfix > div.ser_sm.fl > span");
                    toursType = getToursTypeBySpanClass(toursType);
                }
                else
                {
                    toursNO = getDOMTextBySelector(doc, "#index1200 > div.wrapper_bg > div > div.product_info > div.product_name_tips > span.priduct_no");
                    if (toursNO.contains("编号"))
                    {
                        toursNO = toursNO.replace("编号", "");
                    }
                    
                    toursName = getDOMTextBySelector(doc, "#index1200 > div.wrapper_bg > div > div.product_info > div.product_name_bar > h1");
                    
                    toursType = "自助游";
                }
            }
            
            logger.info("线路编号：" + toursNO);
            logger.info("线路名称：" + toursName);
            logger.info("线路类型：" + toursType);
            
            insertData(preparedData, HBASE_ROWKEY_PREFIX + toursNO, HBASE_TOUR_COLUMN_FAMILY, "route_name", Bytes.toBytes(toursName));
            insertData(preparedData, HBASE_ROWKEY_PREFIX + toursNO, HBASE_TOUR_COLUMN_FAMILY, "route_type", Bytes.toBytes(toursType));
            
            logger.info("-----------------------");
            
            // 将组织好的数据插入HBase
            if (preparedData.size() > 0) 
            {
                Table table = HBaseTools.openTable(HBASE_TOUR_TABLE_NAME);
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
            return;
        }
    }
    
    public static void main(String[] args)
    {
        try
        {
            String url = "http://www.tuniu.com/tours/210177475";
            Document doc = Jsoup.connect(url).get();
            if (doc == null)
            {
                return;
            }
            
            String toursNO = "";
            String toursName = "";
            String toursType = "";
            
            if (doc.select("#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.tours-sub-info.clearfix > div.ser_sm.fl > span.c_f80").size() > 0)
            {
                toursNO = getDOMTextBySelector(doc, "#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.tours-sub-info.clearfix > div.ser_sm.fl > span.c_f80");
                if (toursNO.contains("编号"))
                {
                    toursNO = toursNO.replace("编号", "");
                }
                
                toursName = getDOMTextBySelector(doc, "#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.top_tit > h1");
                
                toursType = getDOMClassBySelector(doc, "#index1200 > div.wrapBody > div > div.mainContent > div.main_top > div.tours-sub-info.clearfix > div.ser_sm.fl > span");
                toursType = getToursTypeBySpanClass(toursType);
            }
            else
            {
                toursNO = getDOMTextBySelector(doc, "#index1200 > div.wrapper_bg > div > div.product_info > div.product_name_tips > span.priduct_no");
                if (toursNO.contains("编号"))
                {
                    toursNO = toursNO.replace("编号", "");
                }
                
                toursName = getDOMTextBySelector(doc, "#index1200 > div.wrapper_bg > div > div.product_info > div.product_name_bar > h1");
                
                toursType = getDOMClassBySelector(doc, "#index1200 > div.wrapper_bg > div > div.product_info > div.product_name_tips > span");
                toursType = getToursTypeBySpanClass(toursType);
            }
            
            logger.info("线路编号：" + toursNO);
            logger.info("线路名称：" + toursName);
            logger.info("线路类型：" + toursType);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
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
     * 从DOM文档中选出指定CSS选择器选出元素的样式.
     * @param doc       页面DOM文档
     * @param selector  选择器字符串
     * @return          指定的元素的Class属性
     */
    private static String getDOMClassBySelector(Document doc, String selector)
    {
        String result = "";
        
        Elements tmpElements = null;
        Element tmpElement = null;
        
        tmpElements = doc.select(selector);
        if (tmpElements != null && tmpElements.size() > 0)
        {
            tmpElement = tmpElements.first();
            result = tmpElement.className().trim();
        }
        
        return result;
    }
    
    /**
     * 根据样式名称来映射旅游的类型.
     * @param className     样式名称
     * @return              具体的线路类型
     */
    private static String getToursTypeBySpanClass(String className)
    {
        String result = "";
        
        switch (className)
        {
            case "style_tour":
                result = "跟团游";
                break;
            case "icon_style_driver":
                result = "自驾游";
                break;
            case "byCar_team":
                result = "跟队自驾";
                break;
            case "self_tours_icon":
                result = "自助游";
                break;
            default:
                break;
        }
        
        return result;
    }
    
}
