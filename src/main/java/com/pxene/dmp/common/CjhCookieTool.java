package com.pxene.dmp.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Jsoup;

public class CjhCookieTool {
	private static final String USERAGENT="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36";
	private static Map<String, String> str2Map(String str){
		Map<String, String> map=new HashMap<>();
		String[] strs=str.split("\\&");
		for (String string : strs) {
			String[] kv=string.split("=");
			if(kv.length!=2){
				map.put(kv[0], "");
				continue;
			}
			map.put(kv[0], kv[1]);
		}
		return map;
	}
	
	public  static void updateCookies(String domainName,Map<String, String> datas,String url){
		Connection con = Jsoup.connect(url);
		con.userAgent(USERAGENT);
		con.method(Method.POST);
		con.timeout(2000);
		
		try {
			Map<String, String> cookies = con.data(datas).execute().cookies();
			if(!new File("fcookie").exists()){
				new File("fcookie").mkdir();
			}
			
			ObjectOutput ob=new ObjectOutputStream(new FileOutputStream("fcookie/"+domainName));
			ob.writeObject(cookies);
			ob.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Map<String, String> getCookie(String domainName){
		Map<String, String> cookie=new HashMap<>();
		try {
			ObjectInputStream objectInputStream=new ObjectInputStream(new FileInputStream("fcookie/"+domainName));
			cookie = (Map<String, String>) objectInputStream.readObject();
			objectInputStream.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cookie;
	}
	
	public static void updateAllCookies(){
		updateCookies("autohome", str2Map("name=qczjcjh123&pwd=49787efcc090d54c64e7c143f2c0318d"), "http://account.autohome.com.cn/Login/ValidIndex");
		updateCookies("bitauto", str2Map("txt_LoginName=18611434755&txt_Password=yccjh12345"), "http://i.yiche.com/ajax/Authenservice/login.ashx");
	}
	
}
