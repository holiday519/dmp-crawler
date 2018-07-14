package com.pxene.dmp.crawler.auto;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4Autosouhu extends BaseCrawler {
	public Crawler4Autosouhu() {
		super("/" + Crawler4Autosouhu.class.getName().replace(".", "/")
				+ ".json");
	}

	private Log logger = LogFactory.getLog(Crawler4Autosouhu.class);

	// 入库所需参数
	private static final String ROWKEY_PREFIX = "00030106_";

	// 汽车类网站用户信息表t_auto_userinfo(user_info列簇)
	private static final String TABLE_NAME_USER = "t_auto_userinfo";
	// 汽车类网站汽车信息表t_auto_autoinfo(auto_info列簇)
	private static final String TABLE_NAME_AUTO = "t_auto_autoinfo";
	// 汽车类网站用户发帖表t_auto_postinfo(post_info列簇)
	private static final String TABLE_NAME_POST = "t_auto_postinfo";

	private static final String FAMILY_NAME_USER = "user_info";
	private static final String FAMILY_NAME_AUTO = "auto_info";
	private static final String FAMILY_NAME_POST = "post_info";

	private final static Pattern FILTERS = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	private static final String CONTAINS = "^http://db.auto.sohu.com/.*$|^http://saa.auto.sohu.com/.*$";
	private static final String LIST_REGEXP = "^http://db.auto.sohu.com/([a-z,A-Z,0-9,-]+)/([0-9]+)$";
	private static final String DETAIL_REGEXP = "^http://db.auto.sohu.com/([a-z,A-Z,0-9,-]+)/([0-9]+)/([0-9]+)$";
	private static final String BBS_REGEXP = "^http://saa.auto.sohu.com/club-([0-9]+)/thread-([0-9]+)-([0-9]+).shtml$";
	private static final String PERSON_REGEXP = "^http://i.auto.sohu.com/user/show/([0-9]+).shtml$|^http://saa.auto.sohu.com/home/([0-9]+)-([0-9]+).shtml$";
	private static final String PERSON2_REGEXP = "^http://saa.auto.sohu.com/home/([0-9]+)-([0-9]+).shtml$";

	private static final String BBS_NAME_SELECTOR = "body > div.wapper980 > div.conmain > div.con-head > h1 > a";
	private static final String BBS_PERSON_SELECTOR = "#floor-0 > div.con-side > a.user-pic";
	private static final String PERSON3_REGEXP = "^http://i.auto.sohu.com/user/show/([0-9]+).shtml$";
	private static final String ORIGINAL_CARD_TIME_SELECTOR = "#body_lz_395331245734027 > div.main-hd > span:nth-child(1) > span";
	private static final String CARD_NAME_SELECTOR = "#topictitle";
	private static final String CARD_COMMENT_SELECTOR = "#body_lz_395331245734027 > div.main-bd";
	private static final String CAR_NAME_SELECTOR = "body > div.top.clearfix > div.top_con > div.top_tit > a:nth-child(4)";
	private static final String CAR_STYLE_NAME = "#carType";
	private static final String PRICE_SELECTOR = "body > div.area.t10 > div.left1 > div:nth-child(1) > div.carInfos > div.r > div > a:nth-child(4) > font";
	private static final String DETAIL_FUEL_SELECTOR = "body > div.area.t10 > div.left1 > div:nth-child(1) > div.carInfos > div.r > ul";
	private static final String DETAIL_FUEL_LI_SELECTOR = "ul > li";
	private static final String FUEL_STR = "工信部综合油耗：";
	private static final String FUEL_STR2 = "工信部综合油耗：(.*)";
	private static final String PQA_STR = "保修政策：";
	private static final String PQA_STR2 = "保修政策：(.*)";
	private static final String GEARBOX_STR = "变速箱：";
	private static final String GEARBOX_STR2 = "变速箱：(.*)";
	private static final String SIZE_STR = "车身尺寸：";
	private static final String SIZE_STR2 = "车身尺寸：(.*)";
	private static final String LIST_DIV_SELECTOR = "#newcar";
	private static final String LIST_DIV_A_SELECTOR = "table > tbody > tr > td.ftdleft > a";
	private static final String USER_LEVEL = "body > div.tw_bbs_daquan_main > div.tw_my_topboxa > dl > dd:nth-child(2) > span:nth-child(1) > i";
	private static final String USER_SEX = "body > div.tw_bbs_daquan_main > div.tw_my_topboxa > dl > dd:nth-child(2) > span:nth-child(2) > em";

	private static final String TEXT = "text";
	private static final String HREF = "href";

	private static final String USER_NICKNAME = "body > div.tw_bbs_daquan_main > div.tw_my_topboxa > dl > dt > a";

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
		System.out.println(url);
		if (url.matches(LIST_REGEXP)) {
			// 访问列表界面
			visitListPage(url);
		} else if (url.matches(DETAIL_REGEXP)) {
			// 访问详细界面
			 visitDetailPage(url);
		} else if (url.matches(BBS_REGEXP)) {
			// 访问论坛界面
			 visitBBSPage(url);
		} else if (url.matches(PERSON_REGEXP)) {
			// 访问用户个人主页
			 visitUserPage(url);
		}
	}

	/**
	 * 访问用户个人主页
	 * 
	 * @param url
	 */
	private void visitUserPage(String url) {
		// 解析用户个人主页
		logger.info("user_url:" + url);
		Document user_doc = connectUrl(url);
		// 获取用户id
		String user_id = "";
		Pattern pattern = Pattern.compile(PERSON2_REGEXP);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			user_id = matcher.group(1);
		}
		// 获取昵称
		String nickname = getTextBySelector(user_doc, USER_NICKNAME, TEXT, null);
		// 获取等级
		String level = getTextBySelector(user_doc, USER_LEVEL, TEXT, null);
		//获取性别
		String sex = getTextBySelector(user_doc, USER_SEX, TEXT, null);
		
		String rowkey = ROWKEY_PREFIX + user_id;
		logger.info("user_rowkey:" + rowkey);
		logger.info("user_id:" + user_id);
		logger.info("nickname:" + nickname);
		logger.info("sex:" + sex);
		logger.info("level:" + level);

		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("nickname", Bytes.toBytes(nickname));
		datas.put("level", Bytes.toBytes(level));
		datas.put("sex", Bytes.toBytes(sex));

		/*
		 * Table table = HBaseTools.openTable(TABLE_NAME_USER); if (table !=
		 * null) { HBaseTools.putColumnDatas(table, rowkey,FAMILY_NAME_USER,
		 * datas); HBaseTools.closeTable(table); }
		 */
	}

	/**
	 * 访问论坛界面
	 * 
	 * @param url
	 */
	private void visitBBSPage(String url) {
		// 解析论坛信息
		logger.info("bbs_url:" + url);
		Document bbs_doc = connectUrl(url);
		// 获取用户id
		String user_id = getTextBySelector(bbs_doc,
				BBS_PERSON_SELECTOR, HREF,
				PERSON3_REGEXP);
		//获取论坛id
		String bbs_id = "";
		//获取帖子id
		String card_id = "";
		Pattern pattern = Pattern.compile(BBS_REGEXP);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			bbs_id = matcher.group(1);
			card_id = matcher.group(2);
		}
		// 获取发帖时间
		String original_card_time = getTextBySelector(
				bbs_doc,
				ORIGINAL_CARD_TIME_SELECTOR,
				TEXT, null);
		// 格式化时间(yyyyMMddHHmmss)
		String time_stamp = StringUtils.date2TimeStamp(original_card_time,
				"yyyy-MM-dd HH:mm:ss");
		String new_card_time = StringUtils.timeStamp2Date(time_stamp,
				"yyyyMMddHHmmss");
		// 获取论坛名称
		String bbs_name = getTextBySelector(bbs_doc, BBS_NAME_SELECTOR, TEXT,
				null);
		// 获取帖子名称
		String card_name = getTextBySelector(bbs_doc, CARD_NAME_SELECTOR, "value",
				null);
		// 获取帖子内容
		String card_content = getTextBySelector(bbs_doc, CARD_COMMENT_SELECTOR,
				TEXT, null);
		// 获取个人页面url
		String person_url = getTextBySelector(bbs_doc, BBS_PERSON_SELECTOR,
				HREF, null);
		myController.addSeed(person_url);

		// 获取帖子内容
		String rowkey = ROWKEY_PREFIX + user_id + "_" + new_card_time;
		logger.info("card_rowkey:" + rowkey);
		logger.info("bbs_id:" + bbs_id + ",bbs_name:" + bbs_name);
		logger.info("post_id:" + card_id + ",post_title:" + card_name);
		logger.info("post_content:" + card_content);

		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		 datas.put("bbs_id", Bytes.toBytes(bbs_id));
		datas.put("bbs_name", Bytes.toBytes(bbs_name.toString()));
		datas.put("post_id", Bytes.toBytes(card_id));
		datas.put("post_title", Bytes.toBytes(card_name));
		datas.put("post_content", Bytes.toBytes(card_content));

		/*
		 * Table table = HBaseTools.openTable(TABLE_NAME_POST); if (table !=
		 * null) { HBaseTools.putColumnDatas(table, rowkey,FAMILY_NAME_POST,
		 * datas); HBaseTools.closeTable(table); }
		 */

	}

	/**
	 * 访问详细界面
	 * 
	 * @param url
	 */
	private void visitDetailPage(String url) {
		// 解析汽车详细页面
		logger.info("detail_url:" + url);
		Document detail_doc = connectUrl(url);
		// 获取车的品牌id
		Pattern p = Pattern.compile(DETAIL_REGEXP);
		Matcher m = p.matcher(url);
		//获取品牌id
		String brand_id = "";
		//获取车id
		String car_id = "";
		while (m.find()) {
			brand_id = m.group(2);
			car_id = m.group(3);
		}
		if (org.apache.commons.lang.StringUtils.isEmpty(brand_id))
			return;
		if (org.apache.commons.lang.StringUtils.isEmpty(car_id))
			return;
		// 获取车名
		String car_name = getTextBySelector(detail_doc, CAR_NAME_SELECTOR,
				TEXT, null);
		// 获取配置名
		String car_style_name = getTextBySelector(detail_doc, CAR_STYLE_NAME,
				TEXT, null);
		// 获取厂商指导价格
		String price = getTextBySelector(detail_doc, PRICE_SELECTOR, TEXT, null);
		// 获得油耗
		String fuel = "";
		// 获得质保
		String pqa = "";
		// 变速箱
		String gearbox = "";
		// 获取车身尺寸(长-宽-高)
		String size = "";
		Elements detail_fuel_elements = detail_doc.select(DETAIL_FUEL_SELECTOR);
		Iterator<Element> detail_fuel_iterator = detail_fuel_elements.iterator();
		while(detail_fuel_iterator.hasNext()){
			Element detail_fuel_element = detail_fuel_iterator.next();
			Elements detail_fuel_li_elements = detail_fuel_element.select(DETAIL_FUEL_LI_SELECTOR);
			Iterator<Element> detail_fuel_li_iterator = detail_fuel_li_elements.iterator();
			while(detail_fuel_li_iterator.hasNext()){
				Element detail_fuel_li_element = detail_fuel_li_iterator.next();
				String li_text = detail_fuel_li_element.text();
				if(li_text.contains(FUEL_STR)){
					fuel = StringUtils.regexpExtract(li_text, FUEL_STR2);
				}else if(li_text.contains(PQA_STR)){
					pqa = StringUtils.regexpExtract(li_text, PQA_STR2);
				}else if(li_text.contains(GEARBOX_STR)){
					gearbox = StringUtils.regexpExtract(li_text, GEARBOX_STR2);
				}else if(li_text.contains(SIZE_STR)){
					size = StringUtils.regexpExtract(li_text, SIZE_STR2);
				}
			}
		}
		
		// rowkey(网站编号+品牌编号+具体编号)
		String rowkey = ROWKEY_PREFIX + brand_id + "_" + car_id;
		String name = car_name;
		String style_name = car_style_name;
		logger.info("style_rowkey:" + rowkey);
		logger.info("name:" + name);
		logger.info("style:" + style_name);
		logger.info("price:" + price);
		logger.info("fuel:" + fuel);
		logger.info("size:" + size);
		logger.info("gearbox:" + gearbox);
		logger.info("pqa:" + pqa);

		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("name", Bytes.toBytes(name));
		datas.put("style", Bytes.toBytes(style_name));
		datas.put("price", Bytes.toBytes(price)); // 车主油耗
		datas.put("fuel", Bytes.toBytes(fuel));
		datas.put("size", Bytes.toBytes(size));
		datas.put("gearbox", Bytes.toBytes(gearbox));
		datas.put("pqa", Bytes.toBytes(pqa));

		/*
		 * Table table = HBaseTools.openTable(TABLE_NAME_AUTO); if (table !=
		 * null) { HBaseTools.putColumnDatas(table, rowkey, FAMILY_NAME_AUTO,
		 * datas); HBaseTools.closeTable(table); }
		 */

	}

	/**
	 * 访问列表界面
	 * 
	 * @param url
	 */
	private void visitListPage(String url) {
		logger.info("list_url:" + url);
		Document list_doc = connectUrl(url);
		Elements list_div_elements = list_doc.select(LIST_DIV_SELECTOR);
		Iterator<Element> list_div_iterator = list_div_elements.iterator();
		while (list_div_iterator.hasNext()) {
			Element list_div_element = list_div_iterator.next();
			Elements list_div_a_elements = list_div_element
					.select(LIST_DIV_A_SELECTOR);
			Iterator<Element> list_div_a_iterator = list_div_a_elements
					.iterator();
			while (list_div_a_iterator.hasNext()) {
				Element list_div_a_element = list_div_a_iterator.next();
				String seed_url = list_div_a_element.attr(HREF);
				myController.addSeed(seed_url);
			}
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
