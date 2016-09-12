package com.pxene.dmp.crawler.ms;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.RedisUtils;
import com.pxene.dmp.common.SolrUtil;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;
import com.pxene.dmp.domain.Article;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Dxy extends BaseCrawler {
	public Crawler4Dxy() {
		super("/" + Crawler4Dxy.class.getName().replace(".", "/") + ".json");
	}

	private Log logger = LogFactory.getLog(Crawler4Dxy.class);

	// 入库所需参数
	private static final String ROWKEY_PREFIX = "00480592001_";

	private static final String TABLE_NAME_POST = "t_dxy_articleinfo";

	private static final String FAMILY_NAME_POST = "article_info";

	private final static Pattern FILTERS = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	private static final String CONTAINS = "^http://(.*).dxy.cn/$|^http://(.*).dxy.cn/article/([0-9]+)$";
	private static final String ARTICLE_REGEXP ="^http://(.*).dxy.cn/article/([0-9]+)$";
	
	private static final String ARTICLE_TITLE_SELECTOR = "#j_article_desc > div.x_box13 > h1";
	private static final String ARTICLE_TIME_SELECTOR = "#j_article_desc > div.x_box13 > div.sum > span:nth-child(1)";
	private static final String REGEXP1 = "^([0-9]{4}-[0-9]{2}-[0-9]{2})$";
	private static final String REGEXP2 = "^([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2})$";
	private static final String ARTICLE_CONTENT_SELECTOR = "#content";
	private static final String AUTHOR_NAME_SELECTOR = "#j_article_desc > div.x_box13 > div.sum > span:nth-child(3)";
	
	private static final String TEXT = "text";
	private static final String HREF = "href";

	@Override
	protected void onContentFetchError(WebURL webUrl) {
		if (proxyConf.isEnable()) {
			String[] params = proxyConf.randomIp().split(":");
			System.getProperties().setProperty("proxySet", "true");
			System.getProperties().setProperty("http.proxyHost", params[0]);
			System.getProperties().setProperty("http.proxyPort", params[1]);
		}
	}

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		return !FILTERS.matcher(href).matches() && href.matches(CONTAINS);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		if (url.matches(ARTICLE_REGEXP)) {
			visitBBSPage(url);
		}
	}

	/**
	 * 访问帖子界面
	 * 
	 * @param url
	 */
	private void visitBBSPage(String url) {
		// 解析论坛信息
		logger.info("article_url:" + url);
		Document article_doc = connectUrl(url);
		//文章id
		String article_id = "";
		String article_sections = "";
		Pattern pattern = Pattern.compile(ARTICLE_REGEXP);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			article_sections = matcher.group(1);
			article_id = matcher.group(2);
		}
		if(article_id.isEmpty()){
			myController.addSeed(url);
			return;
		}
		//文章标题
		String article_title = getTextBySelector(article_doc, ARTICLE_TITLE_SELECTOR, TEXT, null);
		// 发帖时间
		String article1_time = getTextBySelector(article_doc, ARTICLE_TIME_SELECTOR,TEXT, null);
		//补全日期格式
		if(article1_time.matches(REGEXP1)){
			article1_time += " 00:00:00";
		} else if(article1_time.matches(REGEXP2)){
				article1_time += ":00";
		}else if(article1_time.isEmpty()){
			article1_time += "1970-00-00 00:00:00";
		}
		
		// 格式化时间(yyyyMMddHHmm)
		String time_stamp = StringUtils.date2TimeStamp(article1_time,"yyyy-MM-dd HH:mm:ss");
		String article_time = StringUtils.timeStamp2Date(time_stamp,"yyyyMMddHHmmss");
		//文章内容
		String article_content = getTextBySelector(article_doc, ARTICLE_CONTENT_SELECTOR, TEXT, null);
		//作者名称
		String author1_name = getTextBySelector(article_doc, AUTHOR_NAME_SELECTOR, TEXT, null);
		String author_name = StringUtils.regexpExtract(author1_name, "作者：([u4e00-\u9fa5]+)");
		String rowkey = ROWKEY_PREFIX + article_id;
		
		logger.info("rowkey:"+rowkey);
		logger.info("article_id:"+article_id);
		logger.info("article_title:"+article_title);
		logger.info("article_time:"+article_time);
		logger.info("article_content:"+article_content);
		logger.info("author_name:"+author_name);
		logger.info("article_sections:"+article_sections);
		
		//将数据在solr中建立索引
		Article article = new Article();
		article.setId(rowkey);
		article.setAuthor(author_name);
		article.setContent(article_content);
		article.setSections(article_sections);
		article.setTime(StringUtils.stringinsert(article_time, "-", 8));
		article.setTitle(article_title);
		article.setUrl(url);
		SolrUtil.addIndex(article);
		
		//将文章信息存储到hbase中
		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("article_title", Bytes.toBytes(article_title));
		datas.put("article_content", Bytes.toBytes(article_content));
		datas.put("article_url", Bytes.toBytes(url));
		datas.put("article_auther", Bytes.toBytes(author_name));
		datas.put("article_sections", Bytes.toBytes(article_sections));
		datas.put("article_time", Bytes.toBytes(article_time));

		Table table = HBaseTools.openTable(TABLE_NAME_POST);
		if (table != null) {
			HBaseTools.putColumnDatas(table, rowkey, FAMILY_NAME_POST, datas);
			HBaseTools.closeTable(table);
		}
	}


	/**
	 * 根据选择器获得相应的内容
	 * 
	 * @param doc
	 * @param selector
	 * @param attribute
	 * @param regexp
	 * @return
	 */
	private String getTextBySelector(Document doc, String selector,
			String attribute, String regexp) {
		String text = "";
		Elements elements = doc.select(selector);
		Iterator<Element> iterator = elements.iterator();
		while (iterator.hasNext()) {
			Element element = iterator.next();
			String element_comment = "";
			if (attribute.equals(TEXT)) {
				element_comment = element.text();
			} else {
				element_comment = element.attr(attribute);
			}
			if (org.apache.commons.lang.StringUtils.isNotBlank(regexp)) {
				text = StringUtils.regexpExtract(element_comment, regexp);
			} else {
				text = element_comment;
			}
		}
		return text.trim();
	}

}
