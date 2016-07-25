package com.pxene.dmp.crawler.auto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.pxene.dmp.common.StringUtils;
import com.pxene.dmp.crawler.BaseCrawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4BitAuto extends BaseCrawler {
	public Crawler4BitAuto() {
		super("/" + Crawler4BitAuto.class.getName().replace(".", "/") + ".json");
	}

	private Log logger = LogFactory.getLog(Crawler4BitAuto.class);

	// 入库所需参数
	private static final String ROWKEY_PREFIX = "00030006_";

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
	private static final String CONTAINS = "^http://car.bitauto.com/.*$|^http://baa.bitauto.com/.*$|^http://i.yiche.com/.*$";
	private static final String LIST_REGEXP = "^http://car.bitauto.com/([a-z,A-Z,0-9]+)/$";
	private static final String DETAIL_REGEXP = "^http://car.bitauto.com/([a-z,A-Z,0-9]+)/([a-z,A-Z,0-9]+)/$";
	private static final String ASK_REGEXP = "^http://car.bitauto.com/([a-z,A-Z,0-9]+)/ask/$";
	private static final String BBS_REGEXP = "^http://baa.bitauto.com/.*/thread-([0-9]+).html$";
	private static final String PERSON_REGEXP = "^http://i.yiche.com/u([0-9]+)/$";

	private static final String BBS_NAME_SELECTOR = "#TitleForumLink";
	private static final String BBS_PERSON_SELECTOR = "#postleft1 > div.user_photo.firstFloor > a";
	private static final String CARD_NAME_SELECTOR = "#TitleTopicSt";
	private static final String CARD_COMMENT_SELECTOR = "body > div.bt_page > div.postcontbox > div.postcont_list.post_fist > div > div.postright > div.post_text.post_text_sl > div.post_width";
	private static final String BRAND_ID_SELECTOR = "#middleADForCar";
	private static final String CAR_ID_SELECTOR = "#hidCarID";
	private static final String CAR_NAME_SELECTOR = "body > div.bt_page > div > div.col-con > div.title-con > div > h3 > a";
	private static final String CAR_STYLE_NAME = "#car-pop";
	private static final String PRICE_SELECTOR = "#jiaGeDetail > span > em";
	private static final String SPEED_SELECTOR = "#DicCarParameter > div.car_config.car_top_set > table:nth-child(2) > tbody:nth-child(2)";
	private static final String DETAIL_TABLE2_TR_SELECTOR = "tbody > tr";
	private static final String DETAIL_TABLE2_TH_SELECTOR = "tr > th";
	private static final String HIGH_SPEED = "最高车速";
	private static final String FUEL = "综合工况油耗";
	private static final String PQA = "保修政策";
	private static final String GEARBOX = "变速箱";
	private static final String DETAIL_TABLE2_SELECTOR = "#DicCarParameter > div.car_config.car_top_set > table:nth-child(2) > tbody:nth-child(4)";
	private static final String CHANG = "长";
	private static final String KUAN = "宽";
	private static final String GAO = "高";
	private static final String LIST_TABLE_SELECTOR = "#compare_sale > tbody";
	private static final String LIST_TABLE_A_SELECTOR = "tbody > tr > td > div > a";
	private static final String LIST_TABLE_A_HREF_SELECTOR = "/([a-z,A-Z,0-9]+)/([a-z,A-Z,0-9]+)/";
	private static final String USER_CITY = "body > div.mybox_page > div.homepage_box > div.his_infor_box > div.middle_box_ta > ul > li:nth-child(2)";
	private static final String CITY = "地区：(.*)";
	private static final String USER_LEVEL = "body > div.mybox_page > div.homepage_box > div.his_infor_box > div.middle_box_ta > ul > li:nth-child(1)";
	private static final String LEVEL = "等级：(.*)";

	private static final String DOMAIN = "http://car.bitauto.com/";
	private static final String TEXT = "text";
	private static final String HREF = "href";
	private static final String LOVE_CARS_SUFFIX = "car/default.html";
	private static final String GUANZHU_CARS_SUFFIX = "car/guanzhu/";
	private static final String PLAN_CARS_SUFFIX = "car/plan/";

	private static final String USER_NICKNAME = "#avatar_title > strong";
	private static final String CAR = "body > div.mybox_page > div.homepage_box > div.line_box > div.aiche_box";
	private static final String CAR_A = "div.aiche_box > div.mycar_box > div.mycar_photo > a";
	private static final String GUANZHU = "body > div.mybox_page > div.homepage_box > div.line_box > div.guanzhucar_box > ul > li";
	private static final String GUANZHU_A = "li > div.carmodel_box > div.car_link_float > div.button_orange.button_96 > a";
	private static final String PLAN = "body > div.mybox_page > div.homepage_box > div.line_box > div.aiche_box > div";
	private static final String PLAN_A = "div > div.mycar_photo.mycar_140 > a";
	
	private static final String BBS_SELECTOR = "#postleft1 > div.user_photo.firstFloor > a";
	private static final String BBS_CARD_TIME = "body > div.bt_page > div.postcontbox > div.postcont_list.post_fist > div > div.postright > div.post_text.post_text_sl > div.post_fist_title > div.time_box > span";

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
		if (url.matches(LIST_REGEXP)) {
			// 访问列表界面
			visitListPage(url);
		} else if (url.matches(DETAIL_REGEXP) && !url.matches(ASK_REGEXP)) {
			// 访问详细界面
			visitDetailPage(url);
		} else if (url.matches(BBS_REGEXP)) {
			// 访问论坛界面
			// visitBBSPage(url);
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
		Pattern pattern = Pattern.compile(PERSON_REGEXP);
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			user_id = matcher.group(1);
		}
		// 获取昵称
		String nickname = getTextBySelector(user_doc, USER_NICKNAME, TEXT, null);
		// 获取地址
		String city = getTextBySelector(user_doc, USER_CITY, TEXT, CITY);
		// 获取等级
		String level = getTextBySelector(user_doc, USER_LEVEL, TEXT, LEVEL);

		// 获取车库信息
		// 1.获取我的爱车
		Map<String, List> cars = new HashMap<String, List>();
		Document car_doc = connectUrl(url + LOVE_CARS_SUFFIX);
		Elements car_elements = car_doc.select(CAR);
		Iterator<Element> car_iterator = car_elements.iterator();
		while (car_iterator.hasNext()) {
			Element car_element = car_iterator.next();
			Elements car_a_elements = car_element.select(CAR_A);
			Iterator<Element> car_a_iterator = car_a_elements.iterator();
			while (car_a_iterator.hasNext()) {
				Element car_a_element = car_a_iterator.next();
				String car_a_href = car_a_element.attr(HREF);
				String match = "^http://car.bitauto.com/([a-z,A-Z,0-9]+)/m([0-9]+)$";
				Pattern p = Pattern.compile(match);
				Matcher m = p.matcher(car_a_href);
				if (m.find()) {
					String car_id = m.group(1);
					Set<String> car_ids = cars.keySet();
					Iterator<String> car_ids_iterator = car_ids.iterator();
					boolean flag = false;
					while (car_ids_iterator.hasNext()) {
						String next_car_id = car_ids_iterator.next();
						flag = next_car_id == car_id;
					}
					if (!flag) {
						ArrayList<Integer> typeArr = new ArrayList<Integer>();
						typeArr.add(3);// 3:正在开的车
						cars.put(car_id, typeArr);
					} else {
						List typeArr = cars.get(car_id);
						typeArr.add(3);
						cars.put(car_id, typeArr);
					}
				}
			}
		}

		// 2.获取关注的车
		Document guanzhu_doc = connectUrl(url + GUANZHU_CARS_SUFFIX);
		Elements guanzhu_elements = guanzhu_doc.select(GUANZHU);
		Iterator<Element> guanzhu_iterator = guanzhu_elements.iterator();
		while (guanzhu_iterator.hasNext()) {
			Element guanzhu_element = guanzhu_iterator.next();
			Elements guanzhu_a_elements = guanzhu_element.select(GUANZHU_A);
			Iterator<Element> guanzhu_a_iterator = guanzhu_a_elements
					.iterator();
			while (guanzhu_a_iterator.hasNext()) {
				Element guanzhu_a_element = guanzhu_a_iterator.next();
				String guanzhu_href = guanzhu_a_element.attr(HREF);
				String match = "^http://dealer.bitauto.com/zuidijia/nb([0-9]+)/?leads_source=0$";
				Pattern p = Pattern.compile(match);
				Matcher m = p.matcher(guanzhu_href);
				if (m.find()) {
					String car_id = m.group(1);
					Set<String> car_ids = cars.keySet();
					Iterator<String> car_ids_iterator = car_ids.iterator();
					boolean flag = false;
					while (car_ids_iterator.hasNext()) {
						String next_car_id = car_ids_iterator.next();
						flag = next_car_id == car_id;
					}
					if (!flag) {
						ArrayList<Integer> typeArr = new ArrayList<Integer>();
						typeArr.add(2);// 2:关注的车
						cars.put(car_id, typeArr);
					} else {
						List typeArr = cars.get(car_id);
						typeArr.add(2);
						cars.put(car_id, typeArr);
					}
				}
			}
		}

		// 3.获取计划购的车
		Document plan_doc = connectUrl(url + PLAN_CARS_SUFFIX);
		Elements plan_div_elements = plan_doc.select(PLAN);
		Iterator<Element> plan_div_iterator = plan_div_elements.iterator();
		while (plan_div_iterator.hasNext()) {
			Element plan_div_element = plan_div_iterator.next();
			Elements plan_div_a_elements = plan_div_element.select(PLAN_A);
			Iterator<Element> plan_div_a_iterator = plan_div_a_elements
					.iterator();
			while (plan_div_a_iterator.hasNext()) {
				Element plan_div_a_element = plan_div_a_iterator.next();
				String plan_div_a_href = plan_div_a_element.attr(HREF);
				String match = "^http://car.bitauto.com/([a-z,A-Z,0-9]+)/m([0-9]+)/$";
				Pattern p = Pattern.compile(match);
				Matcher m = p.matcher(plan_div_a_href);
				if (m.find()) {
					String car_id = m.group(1);
					Set<String> car_ids = cars.keySet();
					Iterator<String> car_ids_iterator = car_ids.iterator();
					boolean flag = false;
					while (car_ids_iterator.hasNext()) {
						String next_car_id = car_ids_iterator.next();
						flag = next_car_id == car_id;
					}
					if (!flag) {
						ArrayList<Integer> typeArr = new ArrayList<Integer>();
						typeArr.add(4);// 4:计划购的车
						cars.put(car_id, typeArr);
					} else {
						List typeArr = cars.get(car_id);
						typeArr.add(4);
						cars.put(car_id, typeArr);
					}
				}
			}
		}

		Set<String> user_cars = cars.keySet();
		Iterator<String> user_cars_iterator = user_cars.iterator();
		StringBuffer user_cars_sb = new StringBuffer();
		while (user_cars_iterator.hasNext()) {
			String user_cars_str = "";
			String user_cars_id = user_cars_iterator.next();
			user_cars_str += user_cars_id + "(";
			List typeArr = cars.get(user_cars_id);
			for (Object obj : typeArr) {
				Integer car_type = (Integer) obj;
				user_cars_str += car_type + ",";
			}
			user_cars_str = user_cars_str.substring(0,
					user_cars_str.length() - 1);
			user_cars_str += "),";
			user_cars_sb.append(user_cars_str);
		}
		String user_car = "";
		if (user_cars_sb.length() > 0) {
			user_car = user_cars_sb.substring(0, user_cars_sb.length() - 1);
		}

		String rowkey = ROWKEY_PREFIX + user_id;
		logger.info("user_rowkey:" + rowkey);
		logger.info("user_id:" + user_id);
		logger.info("nickname:" + nickname);
		logger.info("city:" + city);
		logger.info("level:" + level);
		logger.info("user_car:" + user_car);

		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("nickname", Bytes.toBytes(nickname));
		datas.put("city", Bytes.toBytes(city));
		datas.put("level", Bytes.toBytes(level));
		datas.put("user_car", Bytes.toBytes(user_car));

		Table table = HBaseTools.openTable(TABLE_NAME_USER);
		if (table != null) {
			HBaseTools.putColumnDatas(table, rowkey, FAMILY_NAME_USER, datas);
			HBaseTools.closeTable(table);
		}
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
		String user_id = getTextBySelector(bbs_doc, BBS_SELECTOR, HREF,
				"^http://i.yiche.com/u([0-9]+)/$");
		// 获取发帖时间
		String original_card_time = getTextBySelector(bbs_doc, BBS_CARD_TIME,
				TEXT, null);
		// 格式化时间(yyyyMMddHHmmss)
		String time_stamp = StringUtils.date2TimeStamp(original_card_time,
				"yyyy-MM-dd HH:mm:ss");
		String new_card_time = StringUtils.timeStamp2Date(time_stamp,
				"yyyyMMddHHmmss");
		// 获取论坛名称
		String bbs_name = getTextBySelector(bbs_doc, BBS_NAME_SELECTOR, TEXT,
				null);
		// 获取帖子id
		String card_id = "";
		Pattern p = Pattern.compile(BBS_REGEXP);
		Matcher m = p.matcher(url);
		if (m.find()) {
			card_id = m.group(1);
		}
		// 获取帖子名称
		String card_name = getTextBySelector(bbs_doc, CARD_NAME_SELECTOR, TEXT,
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
		// logger.info("bbs_id:" + bbs_id + ",bbs_name:" + bbs_name);
		logger.info("post_id:" + card_id + ",post_title:" + card_name);
		logger.info("post_content:" + card_content);

		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		// datas.put("bbs_id", Bytes.toBytes(bbs_id));
		datas.put("bbs_name", Bytes.toBytes(bbs_name.toString()));
		datas.put("post_id", Bytes.toBytes(card_id));
		datas.put("post_title", Bytes.toBytes(card_name));
		datas.put("post_content", Bytes.toBytes(card_content));

		/*
		 * Table table = HBaseTools.openTable(TABLE_NAME_POST); if (table !=
		 * null) { HBaseTools.putColumnDatas(table, rowkey,FAMILY_NAME_POST,
		 * datas); HBaseTools.closeTable(table); }
		 */

		System.out.println("================================bbs_name:"
				+ bbs_name);
		System.out
				.println("================================card_id:" + card_id);
		System.out.println("================================card_name:"
				+ card_name);
		System.out.println("================================card_content:"
				+ card_content);
		System.out.println("================================person_url:"
				+ person_url);
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
		String brand_id = getTextBySelector(detail_doc, BRAND_ID_SELECTOR,
				"adplay_brandid", null);
		if (org.apache.commons.lang.StringUtils.isEmpty(brand_id))
			return;
		// 获取车的id
		String car_id = getTextBySelector(detail_doc, CAR_ID_SELECTOR, "value",
				null);
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
		// 获取最高车速
		String speed = "";
		Elements detail_table_elements = detail_doc.select(SPEED_SELECTOR);
		Iterator<Element> detail_table_iterator = detail_table_elements
				.iterator();
		// 获得油耗
		String fuel = "";
		// 获得质保
		String pqa = "";
		// 变速箱
		String gearbox = "";
		while (detail_table_iterator.hasNext()) {
			Element detail_table_element = detail_table_iterator.next();
			Elements detail_table_tr_elements = detail_table_element
					.select(DETAIL_TABLE2_TR_SELECTOR);
			Iterator<Element> detail_table_tr_iterator = detail_table_tr_elements
					.iterator();
			while (detail_table_tr_iterator.hasNext()) {
				Element detail_table_tr_element = detail_table_tr_iterator
						.next();
				Elements detail_table_tr_th_elements = detail_table_tr_element
						.select(DETAIL_TABLE2_TH_SELECTOR);
				Iterator<Element> detail_table_tr_th_iterator = detail_table_tr_th_elements
						.iterator();
				while (detail_table_tr_th_iterator.hasNext()) {
					Element detail_table_tr_th_element = detail_table_tr_th_iterator
							.next();
					String tr_th_str = detail_table_tr_th_element.text();
					if (tr_th_str.equals(HIGH_SPEED)) {
						Element nextElementSibling = detail_table_tr_th_element
								.nextElementSibling();
						speed = nextElementSibling.text();
					} else {
						if (tr_th_str.equals(FUEL)) {
							Element nextElementSibling = detail_table_tr_th_element
									.nextElementSibling();
							fuel = nextElementSibling.text();
						} else {
							if (tr_th_str.equals(PQA)) {
								Element nextElementSibling = detail_table_tr_th_element
										.nextElementSibling();
								pqa = nextElementSibling.text();
							} else {
								if (tr_th_str.equals(GEARBOX)) {
									Element nextElementSibling = detail_table_tr_th_element
											.nextElementSibling();
									gearbox = nextElementSibling.text();
								}
							}
						}
					}
				}
			}
		}

		// 获取车身尺寸(长-宽-高)
		String size = "";
		String chang = "";
		String kuan = "";
		String gao = "";
		Elements detail_table2_elements = detail_doc
				.select(DETAIL_TABLE2_SELECTOR);
		Iterator<Element> detail_table2_iterator = detail_table2_elements
				.iterator();
		while (detail_table2_iterator.hasNext()) {
			Element detail_table2_element = detail_table2_iterator.next();
			Elements detail_table2_tr_elements = detail_table2_element
					.select(DETAIL_TABLE2_TR_SELECTOR);
			Iterator<Element> detail_table2_tr_iterator = detail_table2_tr_elements
					.iterator();
			while (detail_table2_tr_iterator.hasNext()) {
				Element detail_table2_tr_element = detail_table2_tr_iterator
						.next();
				Elements detail_table2_tr_th_elements = detail_table2_tr_element
						.select(DETAIL_TABLE2_TH_SELECTOR);
				Iterator<Element> detail_table2_tr_th_iterator = detail_table2_tr_th_elements
						.iterator();
				while (detail_table2_tr_th_iterator.hasNext()) {
					Element detail_table2_tr_th_element = detail_table2_tr_th_iterator
							.next();
					String tr_th_str = detail_table2_tr_th_element.text();
					if (tr_th_str.equals(CHANG)) {
						Element nextElementSibling = detail_table2_tr_th_element
								.nextElementSibling();
						chang = nextElementSibling.text();
					} else {
						if (tr_th_str.equals(KUAN)) {
							Element nextElementSibling = detail_table2_tr_th_element
									.nextElementSibling();
							kuan = nextElementSibling.text();
						} else {
							if (tr_th_str.equals(GAO)) {
								Element nextElementSibling = detail_table2_tr_th_element
										.nextElementSibling();
								gao = nextElementSibling.text();
							}
						}
					}
				}
			}
		}
		size = chang + "-" + kuan + "-" + gao;

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
		logger.info("speed:" + speed);
		logger.info("pqa:" + pqa);

		Map<String, byte[]> datas = new HashMap<String, byte[]>();
		datas.put("name", Bytes.toBytes(name));
		datas.put("style", Bytes.toBytes(style_name));
		datas.put("price", Bytes.toBytes(price));
		datas.put("fuel", Bytes.toBytes(fuel));
		datas.put("size", Bytes.toBytes(size));
		datas.put("gearbox", Bytes.toBytes(gearbox));
		datas.put("speed", Bytes.toBytes(speed));
		datas.put("pqa", Bytes.toBytes(pqa));

		Table table = HBaseTools.openTable(TABLE_NAME_AUTO);
		if (table != null) {
			HBaseTools.putColumnDatas(table, rowkey, FAMILY_NAME_AUTO, datas);
			HBaseTools.closeTable(table);
		}

	}

	/**
	 * 访问列表界面
	 * 
	 * @param url
	 */
	private void visitListPage(String url) {
		logger.info("list_url:" + url);
		Document list_doc = connectUrl(url);
		Elements list_table_elements = list_doc.select(LIST_TABLE_SELECTOR);
		Iterator<Element> list_table_iterator = list_table_elements.iterator();
		while (list_table_iterator.hasNext()) {
			Element list_table_element = list_table_iterator.next();
			Elements list_table_a_elements = list_table_element
					.select(LIST_TABLE_A_SELECTOR);
			Iterator<Element> list_table_a_iterator = list_table_a_elements
					.iterator();
			while (list_table_a_iterator.hasNext()) {
				Element list_table_a_element = list_table_a_iterator.next();
				String a_href = list_table_a_element.attr(HREF);
				if (a_href.matches(LIST_TABLE_A_HREF_SELECTOR)) {
					String seed_url = DOMAIN + a_href;
					myController.addSeed(seed_url);
				}
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
