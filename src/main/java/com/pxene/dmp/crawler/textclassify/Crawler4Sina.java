package com.pxene.dmp.crawler.textclassify;

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
	
	private static final int COUNT_LIMIT = 50000;
	
	private static final class NEWS {
		public static final String CODE = "01";
		public static final class SOCIETY {
			public static final String CODE = "01";
			public static final String REGEX = "^http://news\\.sina\\.com\\.cn/s/.*\\.shtml$";
			public static int count = 0;
		}
		public static final class CHINA {
			public static final String CODE = "02";
			public static final String REGEX = "^http://news\\.sina\\.com\\.cn/c/.*\\.shtml$";
			public static int count = 0;
		}
		public static final class WORLD {
			public static final String CODE = "03";
			public static final String REGEX = "^http://news\\.sina\\.com\\.cn/w/.*\\.shtml$";
			public static int count = 0;
		}
		public static final class MILITARY {
			public static final String CODE = "04";
			public static final String REGEX = "^http://mil\\.news\\.sina\\.com\\.cn/.*\\.shtml$";
			public static int count = 0;
		}
	}
	private static final class SPORTS {
		public static final String CODE = "02";
		public static final class FOOTBALL {
			public static final String CODE = "01";
			public static final String REGEX = "^http://sports\\.sina\\.com\\.cn/(?=g|global|china)/.*\\.shtml$";
			public static int count = 0;
		}
		public static final class BASKETBALL {
			public static final String CODE = "02";
			public static final String REGEX = "^http://sports\\.sina\\.com\\.cn/(?=basketball/nba|cba)/.*\\.shtml$";
			public static int count = 0;
		}
		public static final class OTHERS {
			public static final String CODE = "03";
			public static final String REGEX = "^http://sports\\.sina\\.com\\.cn/(?!g|global|china|basketball/nba|cba)/.*\\.shtml$";
			public static int count = 0;
		}
	}
	private static final class FASHION {
		public static final String CODE = "03";
		public static final class STYLE {
			public static final String CODE = "01";
			public static final String REGEX = "^http://fashion\\.sina\\.com\\.cn/s/.*\\.shtml$";
			public static int count = 0;
		}
		public static final class BEAUTY {
			public static final String CODE = "02";
			public static final String REGEX = "^http://fashion\\.sina\\.com\\.cn/b/.*\\.shtml$";
			public static int count = 0;
		}
	}
	
	private static final String TABLE = "t_article_classify";
	private static final String FAMILY = "article";

	public Crawler4Sina() {
		super("/" + Crawler4Sina.class.getName().replace(".", "/") + ".json");
	}

	@Override
	public boolean shouldVisit(Page referringPage, WebURL webURL) {
		String url = webURL.getURL().toLowerCase();
		if (url.matches("")) {
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
