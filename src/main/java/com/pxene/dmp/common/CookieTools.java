package com.pxene.dmp.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;

public class CookieTools {

	public static Map<String, String> get(String url, String... usrpwd) {
		Map<String, String> cookies = null;
		try {
			cookies = Jsoup.connect(url).method(Method.POST)
					.timeout(20000).data(usrpwd).execute().cookies();
		} catch (IOException e) {
			e.printStackTrace();
			cookies = new HashMap<String, String>();
		}
		return cookies;
	}
	
	public static void main(String[] args) throws IOException {
		Map<String, String> cookies = Jsoup.connect("http://account.autohome.com.cn/Login/ValidIndex")
				.method(Method.POST).data("name", "qczjcjh123", "pwd", "49787efcc090d54c64e7c143f2c0318d")
				.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36").execute().cookies();
		System.out.println(cookies);
		System.out.println(DigestUtils.md5Hex("cjh4321"));
	}
}
