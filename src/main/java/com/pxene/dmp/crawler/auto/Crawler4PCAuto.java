package com.pxene.dmp.crawler.auto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;


public class Crawler4PCAuto extends BaseCrawler
{
    private static Logger logger = LogManager.getLogger(Crawler4PCAuto.class.getName());
    
    private final static String STRING_SEPERATOR = ",";
    
    private final static Pattern REGFILTER_EXTENSION = Pattern.compile(".*(\\.(css|js|gif|jp[e]?g|png|mp3|mp4|zip|gz))$");
    private final static Pattern REGFILTER_AUTO_DETAIL_INFO = Pattern.compile("^http[s]?://price\\.pcauto\\.com\\.cn/sg[0-9]*/?$");
    private final static Pattern REGFILTER_AUTO_CONFIG_INFO = Pattern.compile("^http[s]?://price\\.pcauto\\.com\\.cn/sg[0-9]*/config\\.htm[l]?$");
    private final static Pattern REGFILTER_USER_INFO = Pattern.compile("^http[s]?://my\\.pcauto\\.com\\.cn/[0-9]*/?.*$");
    private final static Pattern REGFILTER_BBS_FORUM_INFO = Pattern.compile("^http[s]?://bbs\\.pcauto\\.com\\.cn/?forum-[0-9]*\\.htm[l]?$");
    private final static Pattern REGFILTER_BBS_TOPIC_INFO = Pattern.compile("^http[s]?://bbs\\.pcauto\\.com\\.cn/?topic-[0-9]*\\.htm[l]?$");
    
    private static final String HBASE_ROWKEY_PREFIX = "00030104_";
    
    private static final String HBASE_AUTO_COLUMN_FAMILY = "auto_info";
    private static final String HBASE_USER_COLUMN_FAMILY = "user_info";
    private static final String HBASE_POST_COLUMN_FAMILY = "post_info";
    
    private static final String HBASE_AUTO_TABLE_NAME = "t_auto_autoinfo";
    private static final String HBASE_USER_TABLE_NAME = "t_auto_userinfo";
    private static final String HBASE_POST_TABLE_NAME = "t_auto_postinfo";
    
    private final static Pattern REGFILTER_USER_HOMEPAGE = Pattern.compile("^http[s]?://my\\.pcauto\\.com\\.cn/[0-9]*$");           // TA的主页
    private final static Pattern REGFILTER_USER_FLLWPAGE = Pattern.compile("^http[s]?://my\\.pcauto\\.com\\.cn/[0-9]*/follow$");    // TA的好友：关注网友
    private final static Pattern REGFILTER_USER_FANSPAGE = Pattern.compile("^http[s]?://my\\.pcauto\\.com\\.cn/[0-9]*/fan$");       // TA的好友：TA的粉丝
    
    
    private static JsonParser jsonParser = new JsonParser();
    
    /**
     * 无参构造方法. 用于指定配置文件的路径
     */
    public Crawler4PCAuto()
    {
        super("/" + Crawler4PCAuto.class.getName().replace(".", "/") + ".json");
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
        
        // Auto info
        if (REGFILTER_AUTO_DETAIL_INFO.matcher(href).matches() || REGFILTER_AUTO_CONFIG_INFO.matcher(href).matches())
        {
            return true;
        }
        
        // User info
        if (REGFILTER_USER_INFO.matcher(href).matches()) 
        { 
            return true; 
        }
         
        
        // Forum info
        if (REGFILTER_BBS_FORUM_INFO.matcher(href).matches() || REGFILTER_BBS_TOPIC_INFO.matcher(href).matches())
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
         * For Debug showBasicPageinfo(url, page);
         */
        
        if (REGFILTER_AUTO_CONFIG_INFO.matcher(url).matches())
        {
            getAutoInfo(((HtmlParseData) page.getParseData()).getHtml());
        }
        if (REGFILTER_USER_INFO.matcher(url).matches())
        {
            getUserInfo(page);
        }
        if (REGFILTER_BBS_TOPIC_INFO.matcher(url).matches())
        {
            getBBSInfo(((HtmlParseData) page.getParseData()).getHtml());
        }
    }
    
    private void getAutoInfo(String html)
    {
        Document doc = Jsoup.parse(html);
        if (doc == null)
        {
            return;
        }
        
        Elements breadCrumbs = doc.select("div.position > div.pos-mark > a");
        String serialId = "";
        String serialName = "";
        if (breadCrumbs != null && breadCrumbs.size() > 0)
        {
            serialId = breadCrumbs.last().attr("abs:href");
            serialId = StringUtils.regexpExtract(serialId, "/sg(\\d*)/");
            serialName = breadCrumbs.last().text();
            logger.info("车系: " + serialName + "(" + serialId + ")");
        }
        
        Elements scripts = doc.select("script");
        for (Element script : scripts)
        {
            String content = script.html();
            
            // 如果HTML的<script>代码段中包含var
            // config这样的变量声明，则提取这个变量，并转换成JSON对象，从这个对象中获取汽车的相关参数
            if (content.contains("var config ="))
            {
                String jsConfigStr = StringUtils.regexpExtract(content, "var config = (\\{.*\\});?").trim();
                if (jsConfigStr.length() != 0)
                {
                    // 声明需要保存至HBase的数据的结果集：
                    Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
                    
                    // 解析JavaScript中的config对象
                    JsonObject jsConfigObject = jsonParser.parse(jsConfigStr).getAsJsonObject();
                    JsonArray items = jsConfigObject.getAsJsonObject("body").getAsJsonArray("items");
                    for (JsonElement item : items)
                    {
                        String itemName = item.getAsJsonObject().get("Name").getAsString();
                        JsonArray itemModelExcessIds = item.getAsJsonObject().get("ModelExcessIds").getAsJsonArray();
                        switch (itemName)
                        {
                            case "车型名称":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoModuleId = o.get("Id").getAsString();
                                    String autoModuleName = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoModuleId + " : " + autoModuleName);
                                    
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoModuleId, HBASE_AUTO_COLUMN_FAMILY, "style", Bytes.toBytes(autoModuleName));
                                }
                                break;
                            case "级别":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoLevel = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoLevel);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "level", Bytes.toBytes(autoLevel));
                                }
                                break;
                            case "厂商指导价(元)":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoPrice = o.get("Value").getAsString();
                                    
                                    autoPrice = StringUtils.regexpExtract(autoPrice, "([.\\d]*)万");// 小数点或者数字
                                    
                                    logger.info(itemName + "(万)：" + autoId + " : " + autoPrice);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "price", Bytes.toBytes(autoPrice));
                                }
                                break;
                            case "最高车速(km/h)":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoSpeed = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoSpeed);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "speed", Bytes.toBytes(autoSpeed));
                                }
                                break;
                            case "工信部综合油耗(L/100km)":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoFuel = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoFuel);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "fuel", Bytes.toBytes(autoFuel));
                                }
                                break;
                            case "长×宽×高(mm)":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoSize = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoSize);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "size", Bytes.toBytes(autoSize));
                                }
                                break;
                            case "车体结构":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoStruct = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoStruct);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "struct", Bytes.toBytes(autoStruct));
                                }
                                break;
                            case "整车质保":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoPqa = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoPqa);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "pqa", Bytes.toBytes(autoPqa));
                                }
                                break;
                            case "发动机":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoEngine = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoEngine);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "engine", Bytes.toBytes(autoEngine));
                                }
                                break;
                            case "变速箱":
                                for (JsonElement itemModelExcessId : itemModelExcessIds)
                                {
                                    JsonObject o = itemModelExcessId.getAsJsonObject();
                                    String autoId = o.get("Id").getAsString();
                                    String autoGearbox = o.get("Value").getAsString();
                                    
                                    logger.info(itemName + "：" + autoId + " : " + autoGearbox);
                                    insertData(preparedData, HBASE_ROWKEY_PREFIX + serialId + "_" + autoId, HBASE_AUTO_COLUMN_FAMILY, "gearbox", Bytes.toBytes(autoGearbox));
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    
                    // 解析HTML DOM获取车名
                    String name = doc.select("div.subNav-mark h1").first().text();
                    for (Map.Entry<String, Map<String, Map<String, byte[]>>> rowData : preparedData.entrySet())
                    {
                        insertData(preparedData, rowData.getKey(), HBASE_AUTO_COLUMN_FAMILY, "name", Bytes.toBytes(name));
                    }
                    
                    // 将组织好的数据插入HBase
                    if (preparedData.size() > 0) 
                    {
                        Table table = HBaseTools.openTable(HBASE_AUTO_TABLE_NAME);
                        if (table != null) 
                        {
                            HBaseTools.putRowDatas(table, preparedData);
                            HBaseTools.closeTable(table);
                        }
                    }
                }
                logger.info("--------------------------------------");
            }
        }
    }
    
    private void getUserInfo(Page page)
    {
        String url = page.getWebURL().getURL();
        String html = ((HtmlParseData) page.getParseData()).getHtml();
        Document doc = Jsoup.parse(html);
        if (doc == null)
        {
            return;
        }
        
        // 声明需要保存至HBase的数据的结果集：
        Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
        
        try
        {
            String rowKey = null;
            
            /*
             *  解析基本信息
             */
            if (REGFILTER_USER_HOMEPAGE.matcher(url).matches())
            {
                // === 解析用户基本信息：用户ID、用户性别 ===
                String gender = null;
                String userId = null;
                
                Elements userIndexArefElements = doc.select("#her-index a");
                if (userIndexArefElements != null && userIndexArefElements.size() > 0)
                {
                    gender = userIndexArefElements.first().text();
                    userId = StringUtils.regexpExtract(userIndexArefElements.first().attr("href"), "http://my.pcauto.com.cn/(\\d*)/");
                    
                    // 如果获取用户Id失败，则跳出程序
                    if (userId == null || "".equals(userId))
                    {
                        return;
                    }
                    
                    // 初始化RowKey
                    rowKey = HBASE_ROWKEY_PREFIX + userId;
                    
                    if (gender.contains("她"))
                    {
                        gender = "1";
                    }
                    else
                    {
                        gender = "0";
                    }
                    logger.info("用户ID：" + userId);
                    logger.info("用户性别：" + gender);
                    
                    insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "sex", Bytes.toBytes(gender));
                }
                
                
                // === 解析用户基本信息：用户名、地址、生日 ===
                String userName = "";
                String userArea = "";
                String userBirth = "";
                Elements userConElements = doc.select("div.user-info.clearfix div.user-con span");
                if (userConElements != null && userConElements.size() > 0)
                {
                    for (Element element : userConElements)
                    {
                        String tmpText = element.text();
                        if (tmpText.contains("用户"))
                        {
                            userName = tmpText.substring(tmpText.indexOf(":") + 1, tmpText.length()).trim();
                        }
                        if (tmpText.contains("地区"))
                        {
                            userArea = tmpText.substring(tmpText.indexOf(":") + 1, tmpText.length()).trim();
                        }
                        if (tmpText.contains("生日"))
                        {
                            userBirth = tmpText.substring(tmpText.indexOf(":") + 1, tmpText.length()).trim();
                            userBirth = convertTimeString(userBirth);
                            if (userBirth.contains(":"))
                            {
                                userBirth = userBirth.substring(0, 10);
                            }
                        }
                    }
                    logger.info("用户名：" + userName);
                    logger.info("用户地址：" + userArea);
                    logger.info("用户生日：" + userBirth);
                }
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "name", Bytes.toBytes(userName));
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "city", Bytes.toBytes(userArea));
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "birthday", Bytes.toBytes(userBirth));
                
                
                // === 解析用户正在开的车 === 
                Element carIdElement = doc.select("a[id^=carAttr]").first();
                String carId = "";
                String carModel = "";
                if (carIdElement != null && carIdElement.hasAttr("id"))
                {
                    carId = carIdElement.attr("id").replace("carAttr", "");
                    
                    if (getCarAttr(carId) != null && getCarAttr(carId).has("model"))
                    {
                        carModel = getCarAttr(carId).get("model").getAsString();
                    }
                }
                logger.info("正在开的汽车：" + carModel);
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "cars", Bytes.toBytes(carId));
                
                
                // === 解析用户的等级、是否是VIP === 
                String nickname = "";
                String level = "";
                String vip = "";
                JsonObject userInfo = getUserAttr(userId);
                if (userInfo != null)
                {
                    nickname = userInfo.get("nickname").getAsString();
                    level = userInfo.get("level").getAsString();
                    vip = userInfo.get("vip").getAsString();
                }
                logger.info("用户昵称：" + nickname);
                logger.info("用户等级：" + level);
                logger.info("VIP标志：" + vip);
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "nickname", Bytes.toBytes(nickname));
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "level", Bytes.toBytes(level));
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "vip", Bytes.toBytes(vip));
            }
            
            
            /*
             *  解析关注的网友
             */
            if (REGFILTER_USER_FLLWPAGE.matcher(url).matches())
            {
                Set<String> followerList = buildFriendsList(url, "follow");
                logger.info("followerList: " + org.apache.commons.lang.StringUtils.join(followerList, STRING_SEPERATOR));
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "following", Bytes.toBytes(org.apache.commons.lang.StringUtils.join(followerList, STRING_SEPERATOR)));
            } 
            
            /*
             *  解析TA的粉丝
             */
            if (REGFILTER_USER_FANSPAGE.matcher(url).matches())
            {
                Set<String> fanList = buildFriendsList(url, "fan");
                logger.info("fanList: " + org.apache.commons.lang.StringUtils.join(fanList, STRING_SEPERATOR));
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "followers", Bytes.toBytes(org.apache.commons.lang.StringUtils.join(fanList, STRING_SEPERATOR)));
            }
            
            
            /*
             * TODO 解析想要购买的汽车信息
             */
            if (true)
            {
                insertData(preparedData, rowKey, HBASE_USER_COLUMN_FAMILY, "buy_info", Bytes.toBytes(""));
            }
            
            
            /*
             * 将组织好的数据插入HBase
             */
            if (preparedData.size() > 0) 
            {
                Table table = HBaseTools.openTable(HBASE_USER_TABLE_NAME);
                if (table != null) 
                {
                    HBaseTools.putRowDatas(table, preparedData);
                    HBaseTools.closeTable(table);
                }
            }
        }
        catch (Exception e)
        {
            logger.error(e);
            e.printStackTrace();
            return;
        }
    }
    
    private void getBBSInfo(String html)
    {
        Document doc = Jsoup.parse(html);
        if (doc == null)
        {
            return;
        }
        
        Elements scripts = doc.select("script");
        for (Element script : scripts)
        {
            String content = script.html();
            
            // 如果HTML的<script>代码段中包含var
            // Topic这样的变量定义，则提取这个变量的字符串表示，并转换成JSON对象，从这个对象中获取帖子的相关参数
            if (content.contains("var Topic ="))
            {
                String jsConfigStr = StringUtils.regexpExtract(StringUtils.removeLineBreak(content), "var Topic = (.*?);").trim();
                if (jsConfigStr != null && jsConfigStr.length() != 0)
                {
                    // 声明需要保存至HBase的数据的结果集：
                    Map<String, Map<String, Map<String, byte[]>>> preparedData = new HashMap<String, Map<String, Map<String, byte[]>>>();
                    
                    // 解析JavaScript中的Topic对象
                    JsonObject jsConfigObject = jsonParser.parse(jsConfigStr).getAsJsonObject();
                    
                    String topicId = jsConfigObject.get("topicId").getAsString();
                    String forumId = jsConfigObject.get("forumId").getAsString();
                    String authorId = jsConfigObject.get("authorId").getAsString();
                    String forumName = jsConfigObject.get("forumName").getAsString();
                    logger.info("topicId：" + topicId + ", forumId: " + forumId + ", authorId: " + authorId + ", forumName: " + forumName);
                    
                    String postDate = "";
                    Elements postUserDiv = doc.select("div.user-" + authorId);
                    Element postDateDiv = postUserDiv.first().nextElementSibling();
                    if (postDateDiv != null)
                    {
                        postDate = postDateDiv.text().replaceAll("发表于", "").trim();
                        postDate = Long.toString(str2Timestamp(postDate));
                    }
                    
                    // 为减少抓取数量，只获取当年的帖子
                    String currYearStr = new SimpleDateFormat("yyyy").format(new Date());
                    String postYearStr = "";
                    System.out.println(postDate);
                    if (postDate != null && !"".equals(postDate) && postDate.contains("-"))
                    {
                        postYearStr = postDate.substring(0, postDate.indexOf("-"));
                    }
                    if (!currYearStr.equals(postYearStr))
                    {
                        logger.info("非当年数据，不需要录入。");
                        return;
                    }
                    
                    String topicTitle = "";
                    Elements topicTitleElement = doc.select("title");
                    if (topicTitleElement != null && topicTitleElement.size() > 0)
                    {
                        topicTitle = topicTitleElement.first().text();
                        topicTitle = topicTitle.substring(0, topicTitle.indexOf("_")).trim();
                    }
                    
                    String topicContent = "";
                    Element topicContentElement = doc.select("div.post_main > div.post_message").first();
                    if (topicContentElement != null)
                    {
                        topicContent = topicContentElement.text();
                    }
                    
                    logger.info("postDate=" + postDate + ", topicTitle=" + topicTitle + ", topicContent=" + topicContent + ".");
                    logger.info("==============");
                    
                    
                    insertData(preparedData, HBASE_ROWKEY_PREFIX + authorId + "_" + postDate, HBASE_POST_COLUMN_FAMILY, "bbs_id", Bytes.toBytes(forumId));
                    insertData(preparedData, HBASE_ROWKEY_PREFIX + authorId + "_" + postDate, HBASE_POST_COLUMN_FAMILY, "bbs_name", Bytes.toBytes(forumName));
                    insertData(preparedData, HBASE_ROWKEY_PREFIX + authorId + "_" + postDate, HBASE_POST_COLUMN_FAMILY, "post_id", Bytes.toBytes(topicId));
                    insertData(preparedData, HBASE_ROWKEY_PREFIX + authorId + "_" + postDate, HBASE_POST_COLUMN_FAMILY, "post_title", Bytes.toBytes(topicTitle));
                    insertData(preparedData, HBASE_ROWKEY_PREFIX + authorId + "_" + postDate, HBASE_POST_COLUMN_FAMILY, "post_content", Bytes.toBytes(topicContent));
                    
                    
                    // 将组织好的数据插入HBase
                    if (preparedData.size() > 0) 
                    {
                        Table table = HBaseTools.openTable(HBASE_POST_TABLE_NAME);
                        if (table != null) 
                        {
                            HBaseTools.putRowDatas(table, preparedData);
                            HBaseTools.closeTable(table);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * 调试用代码：Show the origin message
     * 
     * @param url
     * @param page
     */
    public void showBasicPageinfo(String url, Page page)
    {
        logger.info("===================================================");
        logger.info("Fetched URL: " + url);
        if (page.getParseData() instanceof HtmlParseData)
        {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String text = htmlParseData.getText();
            String html = htmlParseData.getHtml();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();
            
            logger.info("Text length: " + text.length());
            logger.info("Html length: " + html.length());
            logger.info("Number of outgoing links: " + links.size());
            logger.info("===================================================\n");
        }
    }
    
    /**
     * 将字符串转换成Unix时间戳.
     * 
     * @param source    原始的字符串，形如：2016-04-17 59:59:59。
     * @return          转换成的long类型的时间戳，形如：1461038399000。
     */
    public static long str2Timestamp(String source)
    {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Date date = null;
        try
        {
            date = simpleDateFormat.parse(source);
            return date.getTime();
        }
        catch (ParseException e)
        {
            return 0;
        }
    }
    
    /**
     * 将"Tue Nov 17 00:00:00 CST 1992"格式的日期转换成"1992-11-17 14:00:00"
     * @param source
     * @return
     */
    private static String convertTimeString(String source)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
        TimeZone tz = TimeZone.getTimeZone("GMT+8");
        sdf.setTimeZone(tz);
        Date s = null;
        try
        {
            s = sdf.parse(source);
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        catch (ParseException e)
        {
            return "";
        }
        return sdf.format(s);
    }
    
    
    /**
     * 解析页面中的粉丝或者关注，将他们的编号保存至一个集合中
     * @param doc
     * @param resultList
     * @param type “关注网友”Tab页：follow；“TA的粉丝”Tab页：fan。
     */
    private static void parseFriendPage(Document doc, Set<String> resultList, String type)
    {
        Elements friendsElements = doc.select("div.news > div > a");
        if (friendsElements != null && friendsElements.size() > 0)
        {
            for (Element element : friendsElements)
            {
                String tmpText = element.attr("abs:href");
                
                if (element.hasClass("current") && tmpText.contains(type))
                {
                    Elements followerElements = doc.select("input.accountId");
                    for (Element followerElement : followerElements)
                    {
                        resultList.add(followerElement.attr("value"));
                    }
                }
            }
        }
    }
    
    
    
    public static void main(String[] args)
    {
        try
        {
            String autoURL1 = "http://price.pcauto.com.cn/sg2143/";
            String autoURL2 = "http://price.pcauto.com.cn/sg2143/config.html";
            testAutoInfo(autoURL1);
            testAutoInfo(autoURL2);
            
            System.out.println(System.getProperty("line.separator") + "------------------------------------------------------------ 这是华丽的分隔线 ------------------------------------------------------------" + System.getProperty("line.separator"));
            
            // 24076832   27677077
            String uId = "27677077";
            String userURL1 = "http://my.pcauto.com.cn/" + uId;
            String userURL2 = "http://my.pcauto.com.cn/" + uId + "/follow";
            String userURL3 = "http://my.pcauto.com.cn/" + uId + "/fan";
            testUserInfo(userURL1);
            testUserInfo(userURL2);
            testUserInfo(userURL3);
            
            System.out.println(System.getProperty("line.separator") + "------------------------------------------------------------ 这是华丽的分隔线 ------------------------------------------------------------" + System.getProperty("line.separator"));
            
            String bbsURL1 = "http://bbs.pcauto.com.cn/forum-14555-2.html";
            String bbsURL2 = "http://bbs.pcauto.com.cn/topic-11168661.html";
            testBBSInfo(bbsURL1);
            testBBSInfo(bbsURL2);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    
    public static void testAutoInfo(String url)
    {
        Document doc = null;
        try
        {
            doc = Jsoup.connect(url).get();
            
            Elements breadCrumbs = doc.select("div.position > div.pos-mark > a");
            String serialId = "";
            String serialName = "";
            if (breadCrumbs != null && breadCrumbs.size() > 0)
            {
                serialId = breadCrumbs.last().attr("abs:href");
                serialId = StringUtils.regexpExtract(serialId, "/sg(\\d*)/");
                serialName = breadCrumbs.last().text();
                logger.info("车系: " + serialName + "(" + serialId + ")");
            }
            
            Elements scripts = doc.select("script");
            for (Element script : scripts)
            {
                String content = script.html();
                
                // 如果HTML的<script>代码段中包含var config这样的变量声明，则提取这个变量，并转换成JSON对象，从这个对象中获取汽车的相关参数
                if (content.contains("var config ="))
                {
                    String jsConfigStr = StringUtils.regexpExtract(content, "var config = (\\{.*\\});?").trim();
                    if (jsConfigStr.length() != 0)
                    {
                        // 解析JavaScript中的config对象
                        JsonObject jsConfigObject = jsonParser.parse(jsConfigStr).getAsJsonObject();
                        JsonArray items = jsConfigObject.getAsJsonObject("body").getAsJsonArray("items");
                        for (JsonElement item : items)
                        {
                            String itemName = item.getAsJsonObject().get("Name").getAsString();
                            JsonArray itemModelExcessIds = item.getAsJsonObject().get("ModelExcessIds").getAsJsonArray();
                            switch (itemName)
                            {
                                case "车型名称":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoModuleId = o.get("Id").getAsString();
                                        String autoModuleName = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoModuleId + " : " + autoModuleName);
                                    }
                                    break;
                                case "级别":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoLevel = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoLevel);
                                    }
                                    break;
                                case "厂商指导价(元)":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoPrice = o.get("Value").getAsString();
                                        
                                        autoPrice = StringUtils.regexpExtract(autoPrice, "([.\\d]*)万");// 小数点或者数字
                                        
                                        logger.info(itemName + "(万)：" + autoId + " : " + autoPrice);
                                    }
                                    break;
                                case "最高车速(km/h)":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoSpeed = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoSpeed);
                                    }
                                    break;
                                case "工信部综合油耗(L/100km)":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoFuel = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoFuel);
                                    }
                                    break;
                                case "长×宽×高(mm)":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoSize = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoSize);
                                    }
                                    break;
                                case "车体结构":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoStruct = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoStruct);
                                    }
                                    break;
                                case "整车质保":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoPqa = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoPqa);
                                    }
                                    break;
                                case "发动机":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoEngine = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoEngine);
                                    }
                                    break;
                                case "变速箱":
                                    for (JsonElement itemModelExcessId : itemModelExcessIds)
                                    {
                                        JsonObject o = itemModelExcessId.getAsJsonObject();
                                        String autoId = o.get("Id").getAsString();
                                        String autoGearbox = o.get("Value").getAsString();
                                        
                                        logger.info(itemName + "：" + autoId + " : " + autoGearbox);
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                        
                        // 解析HTML DOM获取车名
                        String name = doc.select("div.subNav-mark h1").first().text();
                        logger.info("Auto name=" + name);
                    }
                }
            }
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
        
    }
    
    public static void testUserInfo(String url)
    {
        Document doc = null;
        try
        {
            doc = Jsoup.connect(url).get();
        
            /*
             *  解析基本信息
             */
            if (REGFILTER_USER_HOMEPAGE.matcher(url).matches())
            {
                // === 解析用户基本信息：用户ID、用户性别 ===
                String gender = null;
                String userId = null;
                Elements userIndexArefElements = doc.select("#her-index a");
                if (userIndexArefElements != null && userIndexArefElements.size() > 0)
                {
                    gender = userIndexArefElements.first().text();
                    userId = StringUtils.regexpExtract(userIndexArefElements.first().attr("href"), "http://my.pcauto.com.cn/(\\d*)/");
                    
                    if (gender.contains("她"))
                    {
                        gender = "1";
                    }
                    else
                    {
                        gender = "0";
                    }
                    logger.info("用户ID：" + userId);
                    logger.info("用户性别：" + gender);
                }
                
                
                // === 解析用户基本信息：用户名、地址、生日 ===
                String userName = "";
                String userArea = "";
                String userBirth = "";
                Elements userConElements = doc.select("div.user-info.clearfix div.user-con span");
                if (userConElements != null && userConElements.size() > 0)
                {
                    for (Element element : userConElements)
                    {
                        String tmpText = element.text();
                        if (tmpText.contains("用户"))
                        {
                            userName = tmpText.substring(tmpText.indexOf(":") + 1, tmpText.length()).trim();
                        }
                        if (tmpText.contains("地区"))
                        {
                            userArea = tmpText.substring(tmpText.indexOf(":") + 1, tmpText.length()).trim();
                        }
                        if (tmpText.contains("生日"))
                        {
                            userBirth = tmpText.substring(tmpText.indexOf(":") + 1, tmpText.length()).trim();
                            userBirth = convertTimeString(userBirth);
                            if (userBirth.contains(":"))
                            {
                                userBirth = userBirth.substring(0, 10);
                            }
                        }
                    }
                    logger.info("用户名：" + userName);
                    logger.info("用户地址：" + userArea);
                    logger.info("用户生日：" + userBirth);
                }
                
                
                // === 解析用户正在开的车 === 
                Element carIdElement = doc.select("a[id^=carAttr]").first();
                String carId = carIdElement.attr("id").replace("carAttr", "");
                String carModel = getCarAttr(carId).get("model").getAsString();
                logger.info("正在开的汽车：" + carModel);
                
                
                // === 解析用户的等级、是否是VIP === 
                JsonObject userInfo = getUserAttr(userId);
                String nickname = userInfo.get("nickname").getAsString();
                String level = userInfo.get("level").getAsString();
                String vip = userInfo.get("vip").getAsString();
                logger.info("用户昵称：" + nickname);
                logger.info("用户等级：" + level);
                logger.info("VIP标志：" + vip);
            }
            
            
            /*
             *  解析关注的网友
             */
            if (REGFILTER_USER_FLLWPAGE.matcher(url).matches())
            {
                Set<String> followerList = buildFriendsList(url, "follow");
                logger.info("followerList: " + org.apache.commons.lang.StringUtils.join(followerList, STRING_SEPERATOR));
            } 
            
            /*
             *  解析TA的粉丝
             */
            if (REGFILTER_USER_FANSPAGE.matcher(url).matches())
            {
                Set<String> fanList = buildFriendsList(url, "fan");
                logger.info("fanList: " + org.apache.commons.lang.StringUtils.join(fanList, STRING_SEPERATOR));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return;
        }
    }
    
    public static void testBBSInfo(String url)
    {
        Document doc = null;
        try
        {
            doc = Jsoup.connect(url).get();
            
            Elements scripts = doc.select("script");
            for (Element script : scripts)
            {
                String content = script.html();
                
                // 如果HTML的<script>代码段中包含var
                // Topic这样的变量定义，则提取这个变量的字符串表示，并转换成JSON对象，从这个对象中获取帖子的相关参数
                if (content.contains("var Topic ="))
                {
                    String jsConfigStr = StringUtils.regexpExtract(StringUtils.removeLineBreak(content), "var Topic = (.*?);").trim();
                    if (jsConfigStr != null && jsConfigStr.length() != 0)
                    {
                        // 解析JavaScript中的Topic对象
                        JsonObject jsConfigObject = jsonParser.parse(jsConfigStr).getAsJsonObject();
                        
                        String topicId = jsConfigObject.get("topicId").getAsString();
                        String forumId = jsConfigObject.get("forumId").getAsString();
                        String authorId = jsConfigObject.get("authorId").getAsString();
                        String forumName = jsConfigObject.get("forumName").getAsString();
                        logger.info("topicId：" + topicId + ", forumId: " + forumId + ", authorId: " + authorId + ", forumName: " + forumName);
                        
                        String postDate = "";
                        Elements postUserDiv = doc.select("div.user-" + authorId);
                        Element postDateDiv = postUserDiv.first().nextElementSibling();
                        if (postDateDiv != null)
                        {
                            postDate = postDateDiv.text().replaceAll("发表于", "").trim();
                            
                            String currYearStr = new SimpleDateFormat("yyyy").format(new Date());
                            String postYearStr = "";
                            System.out.println(postDate);
                            if (postDate != null && !"".equals(postDate) && postDate.contains("-"))
                            {
                                postYearStr = postDate.substring(0, postDate.indexOf("-"));
                            }
                            if (currYearStr.equals(postYearStr))
                            {
                                System.out.println("是当年数据，需要录入。");
                            }
                            
                            postDate = Long.toString(str2Timestamp(postDate));
                        }
                        
                        String topicTitle = "";
                        Elements topicTitleElement = doc.select("title");
                        if (topicTitleElement != null && topicTitleElement.size() > 0)
                        {
                            topicTitle = topicTitleElement.first().text();
                            topicTitle = topicTitle.substring(0, topicTitle.indexOf("_")).trim();
                        }
                        
                        String topicContent = "";
                        Element topicContentElement = doc.select("div.post_main > div.post_message").first();
                        if (topicContentElement != null)
                        {
                            topicContent = topicContentElement.text();
                        }
                        
                        logger.info("postDate=" + postDate + ", topicTitle=" + topicTitle + ", topicContent=" + topicContent + ".");
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    
    /**
     * 解析页面中的粉丝或者关注，将他们的编号保存至一个集合中.
     * @param url   需要解析的URL
     * @param type  类型：“关注网友”Tab页为follow；“TA的粉丝”Tab页为fan。
     * @return
     * @throws IOException
     */
    public static Set<String> buildFriendsList(String url, String type) throws IOException
    {
        // 创建结果集
        Set<String> resultList = new HashSet<String>();
        
        // 解析当前页面
        Document doc = Jsoup.connect(url).get();
        parseFriendPage(doc, resultList, type);
        
        // 如果解析当前页面存在分页，则需要再对分页继续进行解析
        Elements pageLinkElements = doc.select("div.pcauto_page a");
        for (Element pageLinkElement : pageLinkElements)
        {
            String paginationURL = pageLinkElement.absUrl("href");
            
            Document paginationDocument = Jsoup.connect(paginationURL).get();
            parseFriendPage(paginationDocument, resultList, type);
        }
        
        return resultList;
    }
    
    
    /**
     * 发送Ajax请求（JSONP跨域）：获得汽车属性
     * @param carId 车型编号
     * @return
     * @throws JSONException
     * @throws IOException
     * @throws ParseException
     */
    private static JsonObject getCarAttr(String carId) throws JSONException, IOException, ParseException
    {
        JsonObject result = new JsonObject();
        HttpClient client = new HttpClient();
        
        long timeStamp = new Date().getTime();
        
        String uri = "http://my.pcauto.com.cn/intf/getCarAttr.jsp?callback=jsonp" + timeStamp + "&act=getCarAttr&carId=" + carId;
        
        HttpMethod httpGet = new GetMethod(uri);
        int statusCode = client.executeMethod(httpGet);
     
        if (statusCode == HttpStatus.SC_OK)
        {
            InputStream inputStream = httpGet.getResponseBodyAsStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "GBK"));
            StringBuilder stringBuilder = new StringBuilder();
            String line = "";
            while ((line = br.readLine()) != null)
            {
                stringBuilder.append(line);
            }
            br.close();
            String body = new String(stringBuilder.toString().getBytes(), "UTF-8");
            
            String jsonStr = StringUtils.regexpExtract(body, "(\\{.*\\})");
            result = jsonParser.parse(jsonStr).getAsJsonObject();
        }
        return result;
    }
    
    /**
     * 发送Ajax请求（JSONP跨域）：获得用户属性
     * @param userId 用户编号
     * @return 
     * @throws JSONException
     * @throws IOException
     * @throws ParseException
     */
    private static JsonObject getUserAttr(String userId) throws JSONException, IOException, ParseException
    {
        JsonObject result = new JsonObject();
        HttpClient client = new HttpClient();
        
        long timeStamp = new Date().getTime();
        
        String uri = "http://bbs.pcauto.com.cn/action/user/user_setting_json.jsp?uid=" + userId + "&callback=jsonp" + timeStamp;
        
        HttpMethod httpGet = new GetMethod(uri);
        int statusCode = client.executeMethod(httpGet);
     
        if (statusCode == HttpStatus.SC_OK)
        {
            InputStream inputStream = httpGet.getResponseBodyAsStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "GBK"));
            StringBuilder stringBuilder = new StringBuilder();
            String line = "";
            while ((line = br.readLine()) != null)
            {
                stringBuilder.append(line);
            }
            br.close();
            String body = new String(stringBuilder.toString().getBytes(), "UTF-8");
            
            String jsonStr = StringUtils.regexpExtract(body, "(\\{.*\\})");
            result = jsonParser.parse(jsonStr).getAsJsonObject();
        }
        return result;
    }
    
}
