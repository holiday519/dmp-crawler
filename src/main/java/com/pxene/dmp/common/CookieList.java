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
				Map<String, String> cookie = Jsoup.connect(url).method(Method.POST)
						.timeout(20000).data(usrkey, username, pwdkey, password).execute().cookies();
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
