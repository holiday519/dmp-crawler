package com.pxene.dmp.common;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;

public class CookieTools {
	
	private static final String USERAGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	private static final String ROOT_PATH = "/com/pxene/dmp/crawler/";
	
	/**
	 * 
	 * @param path 生成cookie文件路径
	 * @param url
	 * @param params 登陆参数
	 */
	public static void updateCookie(String path, String url, String params) {
		Connection conn = Jsoup.connect(url).userAgent(USERAGENT).method(Method.POST).timeout(2000);
		ObjectOutput ob = null;
		try {
			Map<String, String> cookie = conn.data(params).execute().cookies();
			ob = new ObjectOutputStream(new FileOutputStream(ROOT_PATH + path));
			ob.writeObject(cookie);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (ob != null) {
				try {
					ob.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static Map<String, String> getCookie(String path){
		Map<String, String> cookie = new HashMap<String, String>();
		try {
			ObjectInputStream objectInputStream=new ObjectInputStream(new FileInputStream("fcookie/"+path));
			cookie = (Map<String, String>) objectInputStream.readObject();
			objectInputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cookie;
	}
	
}
