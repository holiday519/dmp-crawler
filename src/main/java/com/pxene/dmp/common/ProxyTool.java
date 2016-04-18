package com.pxene.dmp.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.pxene.dmp.constant.IPList;

public class ProxyTool {
	private static final String USERAGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	private static Log logger = LogFactory.getLog(ProxyTool.class);

	public static String getIpInfo() {
		List<String> iplist = IPList.elements();
		return iplist.get(new Random().nextInt(iplist.size()));
	}

	public static void main(String[] args) {
		updataIps();
		// System.out.println(getIpInfo());
	}

	public static void updataIps() {
		String baseUrl = "http://www.youdaili.net/Daili/http/list_**.html";
		Set<String> totalIps = new HashSet<>();
		Random random = new Random();
		int i = random.nextInt(50);
		Set<String> ips = new HashSet<>();

		Document doc = getDoc(baseUrl.replace("**", i + ""));
		Elements ippages = doc.select("ul.newslist_line li a");

		String page = ippages.get(random.nextInt(ippages.size())).absUrl("href");
		// System.out.println(page);
		String ipInfo = getDoc(page).select("div.cont_font").text();
		ips = StringUtils.fullRegexpExtract(ipInfo, "\\d+\\.\\d+\\.\\d+\\.\\d+:[\\d]{4}");
		// System.out.println(ips);
		totalIps.addAll(ips);
		logger.info("#######更新ip代理文件#######");
		BufferedWriter writer = null;
		// 如果文件不存在，则创建文件
		File ipFile = new File("iplist.txt");
		if (!ipFile.exists()) {
			try {
				ipFile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// 保存到文件中
		try {
			writer = new BufferedWriter(new FileWriter(new File("iplist.txt"), true));
			for (String ip : totalIps) {
				writer.write(ip + "\r\n");
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// 确保document一定获取成功
	private static Document getDoc(String url) {
		Document doc = null;
		int i = 0;
		while (true) {
			try {
				doc = Jsoup.connect(url).cookies(CjhCookieTool.getCookie("autohome")).userAgent(UATool.getUA()).get();
				break;
			} catch (IOException e) {
				logger.info("抓取失败。。。重来。。。");
				List<String> iplist = IPList.elements();
				String ipstr = iplist.get(new Random().nextInt(iplist.size()));
				System.getProperties().setProperty("proxySet", "true");
				System.getProperties().setProperty("http.proxyHost", ipstr.split(":")[0]);
				System.getProperties().setProperty("http.proxyPort", ipstr.split(":")[1]);
				i++;
			}

			// 防止过多次的死循环
			if (i >= 20) {
				break;
			}

		}
		return doc;
	}

}
