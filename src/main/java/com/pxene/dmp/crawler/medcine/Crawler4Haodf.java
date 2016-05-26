package com.pxene.dmp.crawler.medcine;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.crawler.BaseCrawler;
import com.pxene.dmp.crawler.auto.Crawler4Autohome;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Haodf extends BaseCrawler
{
    private final static Pattern REGFILTER_EXTENSION = Pattern.compile(".*(\\.(css|js|gif|jp[e]?g|png|mp3|mp4|zip|gz))$");
    private final static Pattern REGFILTER_EXPERT_ARTICLES = Pattern.compile("^http[s]?://www\\.haodf\\.com/zhuanjiaguandian/.*\\.htm[l]?$");
    
    private Set<String> failedURLs = new HashSet<String>();
    
    protected Crawler4Haodf(String confPath)
    {
        super("/" + Crawler4Haodf.class.getName().replace(".", "/") + ".json");
    }
    
    /**
     * 这个方法用来指示给定的URL是否需要被抓取.
     */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url)
    {
        String href = url.getURL().toLowerCase();
        //return !FILTERS.matcher(href).matches() && href.matches("^http[s]?://.*\\.*haodf\\.com/?.*$");
        //return !FILTERS.matcher(href).matches() && href.matches("^http[s]?://.*\\.*39\\.net/?.*$");
        //return !FILTERS.matcher(href).matches() && href.matches("^http[s]?://.*\\.*babytree\\.com/?.*$");
        return !REGFILTER_EXTENSION.matcher(href).matches() && REGFILTER_EXPERT_ARTICLES.matcher(href).matches();
        //return !FILTERS.matcher(href).matches() && href.matches("^http[s]?://.*\\.*health\\.sina\\.com\\.cn/?.*$");
        //return !FILTERS.matcher(href).matches() && href.matches("^http[s]?://.*\\.xywy\\.com/?.*$");
        //return !FILTERS.matcher(href).matches() && href.matches("^http[s]?://.*\\.chunyuyisheng\\.com/?.*$");
    }
    
    /**
     * 当一个页面被抓取并准备被你的应用处理时，这个方法会被调用.
     */
    @Override
    public void visit(Page page)
    {
        String url = page.getWebURL().getURL();
        
        /* For Debug
        showBasicPageinfo(url, page);
        */
        
        parseExpertArticles(page);
    }
    
    private void parseExpertArticles(Page page)
    {
        String url = page.getWebURL().getURL();
        
        Document doc = Jsoup.parse(((HtmlParseData) page.getParseData()).getHtml());
        if (doc == null) 
        {
            return;
        }
        
        // 获取文章分类
        Elements pArtDetailCate = doc.select("div.bg_w.mb20 > p.art_detail_cate");
        Element category = pArtDetailCate.first();
        String categoryStr = "未知分类";
        if (category != null)
        {
            categoryStr = category.text();
        }
        else
        {
            System.out.println("[No category]" + url);
            failedURLs.add("[No category]" + url);
        }
        System.out.println("文章分类：" + categoryStr);
        
        // 获取文章标题
        Elements pArtDetailTitle = doc.select("body h1.fn > p");
        Element title = pArtDetailTitle.first();
        String titleStr = "未知标题";
        if (title != null)
        {
            titleStr = title.text();
        }
        else
        {
            System.out.println("[No title]" + url);
            failedURLs.add("[No title]" + url);
        }
        System.out.println("文章标题：" + titleStr);
        
        // 获取文章发布时间（全网时间）
        Elements pArtDetailPubTime = doc.select("body p.pb20.gray2.tc.pt5.fs span[class!=ml20][class!=tc]");
        Element pubTime = pArtDetailPubTime.first();
        String pubTimeStr = "未知时间";
        if (pubTime != null)
        {
            pubTimeStr = pubTime.text();
        }
        else
        {
            System.out.println("[No title]" + url);
            failedURLs.add("[No title]" + url);
        }
        System.out.println("发表时间：" + pubTimeStr);
        
        // 获取文章发布者
        Elements pArtDetailArticleWriter = doc.select("body p.pb20.gray2.tc.pt5.fs span[class=ml20] a");
        Element articleWriter = pArtDetailArticleWriter.first();
        String articleWriterStr = "未知作者";
        if (articleWriter != null)
        {
            articleWriterStr = articleWriter.text();
        }
        else
        {
            System.out.println("[No article writer]" + url);
            failedURLs.add("[No article writer]" + url);
        }
        System.out.println("发表者：" + articleWriterStr);
        
        
        Elements pArtDetailArticlePV = doc.select("body p.pb20.gray2.tc.pt5.fs span[class=ml20] font");
        Element articlePV = pArtDetailArticlePV.first();
        String articlePVStr = "未知次数";
        if (articlePV != null)
        {
            articlePVStr = articlePV.text();
        }
        else
        {
            System.out.println("[No articlePV]" + url);
            failedURLs.add("[No articlePV]" + url);
        }
        System.out.println("浏览数：" + articlePVStr);
        
        // 获取文章内容
        Elements pAriDetailContent = doc.select("body div.article_detail");
        Element content = pAriDetailContent.first();
        String contentRawStr = "未知内容";
        String contentPureStr = "未知内容";
        if (content != null)
        {
            contentRawStr = content.html();
            contentPureStr = content.text();
        }
        else
        {
            System.out.println("[No content]" + url);
            failedURLs.add("[No content]" + url);
        }
        System.out.println(contentPureStr);
        
        
        
        System.out.println();
    }
    
    
    
    /**
     * Show the origin message
     * @param url
     * @param page
     */
    private void showBasicPageinfo(String url, Page page)
    {
        System.out.println("===================================================");
        System.out.println("Fetched URL: " + url);
        if (page.getParseData() instanceof HtmlParseData)
        {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String text = htmlParseData.getText();
            String html = htmlParseData.getHtml();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();
            
            System.out.println("Text length: " + text.length());
            System.out.println("Html length: " + html.length());
            System.out.println("Number of outgoing links: " + links.size());
            System.out.println("===================================================\n");
        }
    }
}
