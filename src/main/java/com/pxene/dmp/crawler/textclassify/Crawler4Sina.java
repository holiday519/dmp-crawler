package com.pxene.dmp.crawler.textclassify;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.nodes.Document;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.crawler.BaseCrawler;
import com.pxene.dmp.crawler.textclassify.SinaConfig.FirstLevel;
import com.pxene.dmp.crawler.textclassify.SinaConfig.SecondeLevel;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Sina extends BaseCrawler {

	private Log log = LogFactory.getLog(Crawler4Sina.class);
	private static final String TABLE = "t_article_classify";
	private static final String FAMILY = "article";
	private FirstLevel[] firstLevels;
	private Map<String, String> filterMap = new HashMap<>();
	private Table table;

	@Override
	public void onStart() {
		firstLevels = SinaConfig.GetConfig();
		for (FirstLevel firstLevel : firstLevels) {
			String firstCode = firstLevel.getNum();
			SecondeLevel[] secondeLevels = firstLevel.getChild();
			for (SecondeLevel secondeLevel : secondeLevels) {
				String secondeCode = secondeLevel.getNum();
				String regex = secondeLevel.getRegex();
				filterMap.put(firstCode + secondeCode, regex);
			}
		}
		table = HBaseTools.openTable(TABLE);
	}

	public Crawler4Sina() {
		super("/" + Crawler4Sina.class.getName().replace(".", "/") + ".json");
	}

	@Override
	public boolean shouldVisit(Page referringPage, WebURL webURL) {
		String url = webURL.getURL().toLowerCase();
		for (Entry<String, String> entry : filterMap.entrySet()) {
			String regex = entry.getValue();
			if (url.matches(regex)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void visit(Page page) {
		// solve TextParseData cannot be cast to
		// edu.uci.ics.crawler4j.parser.HtmlParseData
		if (!(page.getParseData() instanceof HtmlParseData)) {
			return;
		}
		String url = page.getWebURL().getURL();
//		 log.info("<=ee-debug=>" + url);
		for (Entry<String, String> entry : filterMap.entrySet()) {
			String code = entry.getKey();
			String regex = entry.getValue();
			if (url.matches(regex)) {
				writeDown2Hbase(page, code);
				break;
			}
		}

	}

	public void writeDown2Hbase(Page page, String code) {
		String url = page.getWebURL().getURL();
		String html = ((HtmlParseData) page.getParseData()).getHtml();
		Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
		Document doc = parseHtml(html);
		String title = doc.select("#artibodyTitle").text().length() > 0 ? doc.select("#artibodyTitle").text() : doc.select("#main_title").text();
		title = title.length() > 0 ? title : doc.select(".news-title").text();
		title = title.length() > 0 ? title : doc.select(".title").text();

		String content = doc.select("#artibody").text().length() > 0 ? doc.select("#artibody").text() : doc.select("#articleContent").text();
		content = content.length() > 0 ? content : doc.select(".textbox").text();
		content = content.length() > 0 ? content : doc.select(".art-con-left").text();
		if (content.length() == 0 || title.length() == 0) {
			return;
		}
		log.info("<=ee-debug=>" + code + ":::" + url);
		// System.out.println(content); //打印测试
		String rowKey = code + UUID.randomUUID().toString();
		insertData(rowDatas, rowKey, FAMILY, "title", Bytes.toBytes(title));
		insertData(rowDatas, rowKey, FAMILY, "content", Bytes.toBytes(content));

		if (rowDatas.size() > 0) {
			if (table != null) {
				HBaseTools.putRowDatas(table, rowDatas);
			}
		}
	}

	@Override
	public void onBeforeExit() {
		HBaseTools.closeTable(table);
	}

}
