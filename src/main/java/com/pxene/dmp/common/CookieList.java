package com.pxene.dmp.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;

import com.pxene.dmp.common.CrawlerConfig.LoginConf;

public class CookieList {

	private static Map<String, Map<String, String>> cookies = new HashMap<String, Map<String, String>>();
	
	public static void init(String className, LoginConf loginConf) {
		if (loginConf.isEnable()) {
			String url = loginConf.getUrl();
			String usrkey = loginConf.getUsrkey();
			String username = loginConf.getUsername();
			String pwdkey = loginConf.getPwdkey();
			String password = loginConf.getPassword();
			try {
				Map<String,String> cookies_map = new HashMap<String,String>();
				cookies_map.put("JSESSIONID", "00857D119872963655D3558D4421B59B");
				cookies_map.put("__utma", "213824646.835511136.1471850656.1471850656.1471850656.1");
				cookies_map.put("__utmz", "213824646.1471850656.1.1.utmccn=(referral)|utmcsr=dbcenter.cintcm.com|utmcct=/channel-link.jsp|utmcmd=referral");
				Map<String, String> cookie = Jsoup.connect(url).method(Method.POST).cookies(cookies_map)
						.timeout(20000).execute().cookies();
				cookies.put(className, cookie);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static Map<String, String> get(String className) {
		if (cookies.containsKey(className)) {
			return cookies.get(className);
		}
		return new HashMap<String, String>();
	}
}
