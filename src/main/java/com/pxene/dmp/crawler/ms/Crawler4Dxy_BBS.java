package com.pxene.dmp.crawler.ms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Dxy_BBS extends BaseCrawler {
	public Crawler4Dxy_BBS() {
		super("/" + Crawler4Dxy_BBS.class.getName().replace(".", "/") + ".json");
	}

	private Log logger = LogFactory.getLog(Crawler4Dxy_BBS.class);

	// 入库所需参数
	private static final String ROWKEY_PREFIX = "00480601_";

	private static final String TABLE_NAME_POST = "t_medicine_postinfo";

	private static final String FAMILY_NAME_POST = "post_info";

	private final static Pattern FILTERS = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	private static final String CONTAINS = "^http://www.dxy.cn/bbs/index.html$|^http://.*.dxy.cn/bbs/board/([0-9]+)$|^http://.*.dxy.cn/bbs/topic/([0-9]+).*$";
	private static final String BBS_REGEXP ="^http://.*.dxy.cn/bbs/topic/([0-9]+).*$";
	private static final String POST_TIME = "#post_1 > table > tbody > tr > td.tbc > div.conbox > div.rec-link-wrap.clearfix > div:nth-child(2) > div > span:nth-child(1)";
	private static final String USER_ID = "#post_1 > table > tbody > tr > td.tbs > div.avatar > div > span:nth-child(2) > a";
	private static final String POST_CONTENT = "#post_1 > table > tbody > tr > td.tbc";
	private static final String CONTAINS2 = "^http://.*.dxy.cn/bbs/topic/[0-9]+?ppg=.*$";
	
	private static final String REGEXP1 = "^([0-9]{4}-[0-9]{2}-[0-9]{2})$";
	private static final String REGEXP2 = "^([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2})$";
	
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
		return !FILTERS.matcher(href).matches() && href.matches(CONTAINS) && !href.matches(CONTAINS2);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		if (url.matches(BBS_REGEXP)) {
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
		logger.info("bbs_url:" + url);
		Document bbs_doc = connectUrl(url);
		//获取论坛id
		String bbs_id = getTextBySelector(bbs_doc, "#location > a:nth-child(2)", HREF, "http://[A-Za-z]+.dxy.cn/bbs/board/([0-9]+)");
		if(bbs_id.isEmpty()){
			myController.addSeed(url);
			return;
		}
		//获取论坛名称
		String bbs_name =getTextBySelector(bbs_doc, "#location > a:nth-child(2)", "title", null);
		//获取帖子id
		String post_id = "";
		Pattern pattern = Pattern.compile(BBS_REGEXP);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			post_id = matcher.group(1);
		}
		if(post_id.isEmpty()){
			myController.addSeed(url);
			return;
		}
		//获取帖子标题
		String post_title = getTextBySelector(bbs_doc, "#postview > table > tbody > tr > th > h1", TEXT, null);
		//获取帖子发表时间
		String post1_time = getTextBySelector(bbs_doc, POST_TIME, TEXT, null);
		//补全日期格式
		if(post1_time.matches(REGEXP1)){
			post1_time += " 00:00:00";
		} else if(post1_time.matches(REGEXP2)){
			post1_time += ":00";
		}else if(post1_time.isEmpty()){
			post1_time += "1970-00-00 00:00:00";
		}
		// 格式化时间(yyyyMMddHHmm)
		String time_stamp = StringUtils.date2TimeStamp(post1_time,"yyyy-MM-dd HH:mm:ss");
		String post_time = StringUtils.timeStamp2Date(time_stamp,"yyyyMMddHHmmss");
		//获取发帖人名称
		String author_id = getTextBySelector(bbs_doc, USER_ID, "data-userid", null);
		if(author_id.isEmpty()){
			myController.addSeed(url);
			return;
		}
		//获取帖子内容
		String post_content1 =getTextBySelector(bbs_doc, POST_CONTENT, TEXT, null);
		String post_content = StringUtils.regexpExtract(post_content1, "1楼 (.*)");
		String rowkey = ROWKEY_PREFIX + author_id + "_" + post_time;
		
		logger.info("rowkey:"+rowkey);
		logger.info("bbs_id:"+bbs_id);
		logger.info("bbs_name:"+bbs_name);
		logger.info("post_id:"+post_id);
		logger.info("post_title:"+post_title);
		logger.info("post_time:"+post_time);
		logger.info("post_content:"+post_content);
		
		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("bbs_id", Bytes.toBytes(bbs_id));
		datas.put("bbs_name", Bytes.toBytes(bbs_name));
		datas.put("post_id", Bytes.toBytes(post_id));
		datas.put("post_title", Bytes.toBytes(post_title));
		datas.put("post_content", Bytes.toBytes(post_content));
		datas.put("post_url", Bytes.toBytes(url));
		datas.put("post_time", Bytes.toBytes(post_time));

		Table table = HBaseTools.openTable(TABLE_NAME_POST);
		if (table != null) {
			HBaseTools.putColumnDatas(table, rowkey, FAMILY_NAME_POST, datas);
			HBaseTools.closeTable(table);
		}
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	protected Document connectUrl(String url) {
		Document doc = null;
		try {
			doc = Jsoup.connect(url).userAgent("chrome1").timeout(50000).get();
			String content = doc.toString();
			if (content.contains("发现以下错误,无法继续处理你的请求:")) {
				if (proxyConf.isEnable()) {
					logger.info("已刚换ip");
					Map<String,String> hostAndPort = proxyConf.getRandomIp();
					System.getProperties().setProperty("proxySet", "true");
					System.getProperties().setProperty("http.proxyHost",
							hostAndPort.get("host"));
					System.getProperties().setProperty("http.proxyPort",
							hostAndPort.get("port"));
				}
				int random = (int) (1 + Math.random() * (100 - 1 + 1));
				doc = Jsoup.connect(url).userAgent("chrome" + random)
						.timeout(20000).get();
				connectUrl(url);
			}
		} catch (IOException e) {
			logger.error("请求URL超时，已发送重新请求，URL:"+url);
			myController.addSeed(url);
		}

		return doc;
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
