package com.pxene.dmp.crawler.textclassify.sports;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.nodes.Document;

import com.pxene.dmp.common.HBaseTools;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Sina extends BaseCrawler {
	
	private Log log = LogFactory.getLog(Crawler4Sina.class);
	
	private static final String FILTER_REGEX = "^http://sports\\.sina\\.com\\.cn/.+/doc-(.+)\\.shtml$";
	
	private static final String TABLE = "zhengyi:article_sports";
	private static final String FAMILY = "article";

	public Crawler4Sina() {
		super("/" + Crawler4Sina.class.getName().replace(".", "/") + ".json");
	}

	@Override
	public boolean shouldVisit(Page referringPage, WebURL webURL) {
		String url = webURL.getURL().toLowerCase();
		if (url.matches(FILTER_REGEX)) {
			return true;
		}
		return false;
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		log.info("<=ee-debug=>" + url);
		String html = ((HtmlParseData) page.getParseData()).getHtml();
		Map<String, Map<String, Map<String, byte[]>>> rowDatas = new HashMap<String, Map<String, Map<String, byte[]>>>();
		Document doc = parseHtml(html);
		String title = doc.select("#artibodyTitle").get(0).text();
		String content = doc.select("#artibody").get(0).text();
		String rowKey = UUID.randomUUID().toString();
		insertData(rowDatas, rowKey, FAMILY, "title", Bytes.toBytes(title));
		insertData(rowDatas, rowKey, FAMILY, "content", Bytes.toBytes(content));
		if (rowDatas.size() > 0) {
			Table table = HBaseTools.openTable(TABLE);
			if (table != null) {
				HBaseTools.putRowDatas(table, rowDatas);
				HBaseTools.closeTable(table);
			}
		}
	}
	
}
