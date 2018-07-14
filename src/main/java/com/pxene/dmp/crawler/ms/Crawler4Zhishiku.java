package com.pxene.dmp.crawler.ms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.example.MyApp.MyController;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Tag;
import org.jsoup.select.Elements;

import com.alibaba.fastjson.JSONObject;
import com.pxene.dmp.common.ConfigUtil;
import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.common.SolrUtil;
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;
import com.pxene.dmp.crawler.ms.domain.Article;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Zhishiku extends BaseCrawler {
	public Crawler4Zhishiku() {
		super("/" + Crawler4Zhishiku.class.getName().replace(".", "/") + ".json");
	}

	private Log logger = LogFactory.getLog(Crawler4Zhishiku.class);

	// 入库所需参数
	private static final String TABLE_NAME_POST = "c_cec_article";

	private static final String FAMILY_NAME_POST = "article_info";

	private final static Pattern FILTERS = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	private static final String DETAIL_REGEXP = "^http://www\\.tcminformatics\\.org/wiki/index\\.php/baike/search\\?kw=.*$";

	public static final String URL_PRF = "http://www.tcminformatics.org/wiki/index.php/baike/search?kw=";

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
		return !FILTERS.matcher(href).matches() && href.matches(DETAIL_REGEXP);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		if (url.matches(DETAIL_REGEXP)) {
			visitBBSPage(url);
		}
	}

	/**
	 * 访问帖子界面
	 * 
	 * @param url
	 */
	private void visitBBSPage(String url) {
		System.out.println("detail_url:" + url);
		//整理url
		String[] urls = url.split("=");
		String rowkey = "";
		if (urls.length == 2) {
			rowkey = urls[1];
		}
		if (rowkey.isEmpty())
			return;
		String valueOfName = ConfigUtil.getByKey(rowkey);
		if (valueOfName.isEmpty())
			return;
		url = urls[0] +"="+ valueOfName;
		
		Document detail_doc = connectUrl(url);
		if (detail_doc.toString().contains("未能找到该词条"))
			return;
		// 获取知识库标题
		String title = getTextBySelector(detail_doc,
				"#title > div > div > h1 > font", TEXT, null);
		Elements divs_elements = detail_doc.select("#title > div > div > font");
		Iterator<Element> divs_iterator = divs_elements.iterator();
		// 获取概述
		String summary = "";
		// 获取段落标题
		String stage_title = "";
		Map<Object, Object> info_map = new HashMap<Object, Object>();
		while (divs_iterator.hasNext()) {
			Element divs_element = divs_iterator.next();
			Elements summary_elements = divs_element.select("font > div > h4");
			Iterator<Element> summary_iterator = summary_elements.iterator();
			if (summary_iterator.hasNext()) {
				Map<String, String> summary_value_map = new HashMap<String, String>();
				Element summary_element = summary_iterator.next();
				summary = summary_element.text();
				Elements summary_tr_elements = divs_element
						.select("font > div > table > tbody > tr");
				Iterator<Element> summary_tr_iterator = summary_tr_elements
						.iterator();
				while (summary_tr_iterator.hasNext()) {
					int num = 1;
					Element summary_tr_element = summary_tr_iterator.next();
					Elements summary_tr_td_elements = summary_tr_element
							.select("tr > td");
					Iterator<Element> summary_tr_td_iterator = summary_tr_td_elements
							.iterator();
					String key = "";
					String value = "";
					while (summary_tr_td_iterator.hasNext()) {
						Element summary_tr_td_element = summary_tr_td_iterator
								.next();
						String keyOrValue = summary_tr_td_element.text();
						if (num % 2 == 0) {
							value = keyOrValue.replaceAll("\"", "'");
							summary_value_map.put(key, value);
						} else {
							key = keyOrValue;
						}
						num += 1;
					}
				}
				info_map.put(summary, summary_value_map);
			}

			Elements div_elements = divs_element
					.select("font > div.panel,.panel-default");
			Iterator<Element> div_iterator = div_elements.iterator();
			while (div_iterator.hasNext()) {
				Map<String, Object> stage_map = new HashMap<String, Object>();
				Element div_element = div_iterator.next();
				Elements stage_title_elements = div_element
						.select("div > div.panel-heading > strong");
				Iterator<Element> stage_title_iterator = stage_title_elements
						.iterator();
				while (stage_title_iterator.hasNext()) {
					Element stage_title_element = stage_title_iterator.next();
					stage_title = stage_title_element.text();
				}
				Elements stage_content_elements = div_element
						.select("div > div.panel-body");
				Iterator<Element> stage_content_iterator = stage_content_elements
						.iterator();
				while (stage_content_iterator.hasNext()) {
					Element stage_content_element = stage_content_iterator
							.next();
					String stage_content_html = stage_content_element.html();
					String[] stage_content_htmls = stage_content_html
							.split("<hr>");
					for (String kv : stage_content_htmls) {
						String new_kv = kv.replaceAll("<br>", "&&");
						Element element = Jsoup.parse(new_kv);
						String kv_str = element.text();
						String[] kv_strs = kv_str.split(":");
						if (kv_strs.length > 1) {
							String key = kv_strs[0];
							String value = kv_strs[1];
							String[] values = value.split("&&");
							List<String> valueList = Arrays.asList(values);
							List<String> newValueList = new ArrayList<String>();
							for(String v : valueList){
								newValueList.add(v.replaceAll("\"", "'"));
							}
							stage_map.put(key, newValueList);
							info_map.put(stage_title, stage_map);
						} else {
							String value = kv_strs[0].replaceAll("\"", "'");
							info_map.put(stage_title, value);
						}
					}
				}
			}
		}

		String content = JSONObject.toJSONString(info_map);
		String time = StringUtils.timeStamp2Date(
				(System.currentTimeMillis() / 1000) + "", "yyyyMMddHHmmss");

		logger.info("title:" + title);
		logger.info("content:" + content);
		logger.info("url:" + url);
		logger.info("time:" + time);
		
		//将数据在solr中建立索引
		Article article = new Article();
		article.setId(rowkey);
		article.setContent(content);
		article.setTime(StringUtils.stringinsert(time, "-", 8));
		article.setTitle(title);
		article.setUrl(url);
		SolrUtil.addIndex(article);

		// 将文章信息存储到hbase中
		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("article_title", Bytes.toBytes(title));
		datas.put("article_content", Bytes.toBytes(content));
		datas.put("article_url", Bytes.toBytes(url));
		datas.put("article_time", Bytes.toBytes(time));

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
