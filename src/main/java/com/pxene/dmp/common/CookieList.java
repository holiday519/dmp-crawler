package com.pxene.dmp.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;

public class CookieList {

	private static Map<String, Map<String, String>> cookies = new HashMap<String, Map<String, String>>();
	
	public static Map<String, String> get(String url, String... usrpwd) {
		if (!cookies.containsKey(url)) {
			synchronized (CookieList.class) {
				if (!cookies.containsKey(url)) {
					Map<String, String> cookie = null;
					try {
						cookie = Jsoup.connect(url).method(Method.POST)
								.timeout(20000).data(usrpwd).execute().cookies();
						cookies.put(url, cookie);
					} catch (IOException e) {
						e.printStackTrace();
						cookies.put(url, new HashMap<String, String>());
					}
				}
			}
		}
		return cookies.get(url);
	}
}
