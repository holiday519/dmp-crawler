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
import com.pxene.dmp.common.SolrUtil;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;
import com.pxene.dmp.domain.Article;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Iiyi extends BaseCrawler {
	public Crawler4Iiyi() {
		super("/" + Crawler4Iiyi.class.getName().replace(".", "/") + ".json");
	}

	private Log logger = LogFactory.getLog(Crawler4Iiyi.class);

	// 入库所需参数
	private static final String ROWKEY_PREFIX = "00480591001_";

	private static final String TABLE_NAME_POST = "t_medicine_postinfo";

	private static final String FAMILY_NAME_POST = "post_info";

	private final static Pattern FILTERS = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	private static final String CONTAINS = "^http://bbs.iiyi.com/forum.php?gid=228$|^http://bbs.iiyi.com/forum-([0-9]+)-([0-9]+).html$|^http://bbs.iiyi.com/thread-([0-9]+)-([0-9]+).html$";
	private static final String POST_REGEXP ="^http://bbs.iiyi.com/thread-([0-9]+)-[0-9]+.html$";
	private static final String POST_CONTENT = "div > div > table > tbody > tr > td.plc > div.pct > div.pcb > div.t_fsz > table > tbody > tr > td > font";


	private static final String POST_TIME_SELECTOR = "div > div > table.plhin > tbody > tr > td.plc > div.pi > div.pti > div.authi > em";
	private static final String POST_AUTHOR_SELECTOR = "div > div > table.plhin > tbody > tr > td.plc > div.pi > div.pti > div.authi > a";
	private static final String POST_AUTHOR_REGEXP = "forum\\.php\\?mod=viewthread&tid=[0-9]+&page=[0-9]+&authorid=([0-9]+)";
	
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
		if (url.matches(POST_REGEXP)) {
			visitBBSPage(url);
		}
	}

	/**
	 * 访问帖子界面
	 * 
	 * @param url
	 */
	private void visitBBSPage(String url) {
		// 解析帖子信息
		logger.info("post_url:" + url);
		Document bbs_doc = connectUrl(url);
		//获取板块id
		String BBS_ID_SELECTOR = "#pt > div > a:nth-child(7)";
		String bbs_id = getTextBySelector(bbs_doc, BBS_ID_SELECTOR, HREF, "forum-([0-9]+)-1.html");
		if(bbs_id.isEmpty()){
			myController.addSeed(url);
			return;
		}
		//获取板块名称
		String bbs_name = getTextBySelector(bbs_doc, BBS_ID_SELECTOR, TEXT, null);
		//获取帖子id
		String post_id ="";
		Pattern pattern = Pattern.compile(POST_REGEXP);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			post_id = matcher.group(1);
		}
		if(post_id.isEmpty()){
			myController.addSeed(url);
			return;
		}
		//获取pid和帖子内容
		String post_pid = "";
		String post_content = "";
		//获取发帖时间
		String original_card_time = "";
		//获取作者id
		String post_author_id = "";
		Elements post_elements = bbs_doc.select("#postlist");
		Iterator<Element> post_iterator = post_elements.iterator();
		while(post_iterator.hasNext()){
			Element post_element = post_iterator.next();
			Elements post_div_elements = post_element.select(POST_CONTENT);
			Iterator<Element> post_div_iterator = post_div_elements.iterator();
			while(post_div_iterator.hasNext()){
				Element post_div_element = post_div_iterator.next();
				post_content = post_div_element.text();
				post_pid = "0";
			}
			Elements post_time_elements = post_element.select(POST_TIME_SELECTOR);
			Iterator<Element> post_time_iterator = post_time_elements.iterator();
			while(post_time_iterator.hasNext()){
				Element post_time_element = post_time_iterator.next();
				original_card_time = post_time_element.text();
				original_card_time = StringUtils.regexpExtract(original_card_time, "发表于 (.*)");
			}
			Elements post_author_elements = post_element.select(POST_AUTHOR_SELECTOR);
			Iterator<Element> post_author_iterator = post_author_elements.iterator();
			while(post_author_iterator.hasNext()){
				Element post_author_element = post_author_iterator.next();
				String post_author = post_author_element.attr(HREF);
				Pattern pattern_author = Pattern.compile(POST_AUTHOR_REGEXP);
				Matcher matcher_author = pattern_author.matcher(post_author);
				if (matcher_author.find()) {
					post_author_id = matcher_author.group(1);
				}
				if(!post_author_id.isEmpty()) break;
			}
		}
		if(post_pid.isEmpty()) return;
		//获取帖子标题
		String post_title = getTextBySelector(bbs_doc, "#thread_subject", TEXT, null);
		// 格式化时间(yyyyMMddHHmmss)
		String time_stamp = StringUtils.date2TimeStamp(original_card_time+":00",
				"yyyy-MM-dd HH:mm:ss");
		String new_card_time = StringUtils.timeStamp2Date(time_stamp,
				"yyyyMMddHHmmss");
		String rowkey = ROWKEY_PREFIX + post_author_id + "_" + new_card_time;

		logger.info("rowkey:"+rowkey);
		logger.info("bbs_id:"+bbs_id);
		logger.info("bbs_name:"+bbs_name);
		logger.info("post_id:"+post_id);
		logger.info("post_title:"+post_title);
		logger.info("post_content:"+post_content);
		logger.info("post_url:"+url);
		logger.info("post_pid:"+post_pid);
		logger.info("post_time:"+StringUtils.stringinsert(new_card_time, "-", 8));
		logger.info("post_author_id:"+post_author_id);
		
		//将数据在solr中建立索引
		Article article = new Article();
		article.setId(rowkey);
		article.setTitle(post_title);
		article.setContent(post_content);
		article.setTime(StringUtils.stringinsert(new_card_time, "-", 8));
		SolrUtil.addIndex(article);
		
		
		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("bbs_id", Bytes.toBytes(bbs_id));
		datas.put("bbs_name", Bytes.toBytes(bbs_name));
		datas.put("post_id", Bytes.toBytes(post_id));
		datas.put("post_title", Bytes.toBytes(post_title));
		datas.put("post_content", Bytes.toBytes(post_content));
		datas.put("post_url", Bytes.toBytes(url));
		datas.put("post_pid", Bytes.toBytes(post_pid));
		datas.put("post_time", Bytes.toBytes(new_card_time));

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
