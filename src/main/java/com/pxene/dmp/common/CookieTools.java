package com.pxene.dmp.common;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

/**
 * cookie工具类
 * 
 * @author xuliuming
 *
 */
public class CookieTools {
	private final static String LOGIN_FILE_NAME = "autohome_login.properties";
	private final static String COOKIES_FILE_NAME = "cookies.json";
	private final static String LOGIN_URL = "http://account.autohome.com.cn/Login/ValidIndex";
	private final static Gson gson = new Gson();

	/**
	 * 模拟登录
	 * 
	 * @param loginFileName登录参数所在的配置文件名称
	 * @param loginUrl登陆请求的url
	 * @return 登陆后的cookies
	 */
	public static Map<String, String> login(String loginFileName, String loginUrl) {
		try {
			Connection conns = Jsoup.connect(loginUrl);
			Response response = conns.timeout(5000).ignoreContentType(true).method(Method.POST)
					.data(getLoginData(loginFileName)).execute();
			System.out.println(response.body());
			return response.cookies();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 读取配置文件中的cookie信息
	 * 
	 * @return 配置文件中的cookies
	 */
	public static Map<String, String> loadCookies(String cookiesFileName) {
		return gson.fromJson(
				new JsonReader(new InputStreamReader(CookieTools.class.getResourceAsStream("/" + cookiesFileName))),
				Map.class);
	}

	/**
	 * 更新cookies
	 * 
	 * @param loginFileName登陆参数所在文件的名称
	 * @param loginUrl登录请求的url
	 * @param cookiesFileNamecookies所在的配置文件的路径
	 */
	public static void update(String loginFileName, String loginUrl, String cookiesFileName) {
		FileWriter fw = null;
		try {
			fw = new FileWriter("/" + cookiesFileName);
			fw.write(gson.toJson(login(loginFileName, loginUrl)));
			Logger.getLogger(CookieTools.class).info("更新cookies " + loginFileName + " 成功！");
		} catch (IOException e) {
			e.printStackTrace();
			Logger.getLogger(CookieTools.class).error("更新cookies " + loginFileName + " 失败！");
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 读取配置文件中的登录参数
	 * 
	 * @param loginFileName配置文件的名称
	 * @return 封装了登录信息的一个map
	 */
	public static Map<String, String> getLoginData(String loginFileName) {
		// 读取配置文件中的参数信息
		Properties properties = new Properties();
		try {
			properties.load(new InputStreamReader(CookieTools.class.getResourceAsStream("/" + loginFileName)));
			Map<String, String> loginPramas = new HashMap<String, String>((Map) properties);
			return loginPramas;
		} catch (IOException e) {
			e.printStackTrace();
			Logger.getLogger(CookieTools.class).error("读取配置文件 " + loginFileName + " 失败！");
		}
		return null;
	}

	public static void main(String[] args) {
		System.out.println(gson.toJson(login(LOGIN_FILE_NAME, LOGIN_URL)));
	}
}
