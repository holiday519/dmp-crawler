package com.pxene.dmp.crawler.textclassify;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.pxene.dmp.crawler.textclassify.SinaConfig.FirstLevel;
import com.pxene.dmp.crawler.textclassify.SinaConfig.SecondeLevel;

public class SinaConfig {

	public static FirstLevel[] GetConfig() {
		JsonReader reader = new JsonReader(new InputStreamReader(SinaConfig.class.getResourceAsStream("/com/pxene/dmp/crawler/textclassify/sinaConfig.json")));
		// 解析数组
		FirstLevel[] paramObjs = new Gson().fromJson(reader, FirstLevel[].class);
		return paramObjs;
	}

	public static void main(String[] args) throws Exception {
		FirstLevel[] firstLevels;
		Map<String, String> filterMap = new HashMap<>();
		List<String> urls=FileUtils.readLines(new File("./url.txt"));
//		List<String>  urls=IOUtils.readLines(SinaConfig.class.getResourceAsStream("/com/pxene/dmp/crawler/textclassify/url.txt"));
		firstLevels = SinaConfig.GetConfig();
		for (FirstLevel firstLevel : firstLevels) {
			String firstCode = firstLevel.getNum();
			SecondeLevel[] secondeLevels = firstLevel.getChild();
			for (SecondeLevel secondeLevel : secondeLevels) {
				String secondeCode = secondeLevel.getNum();
				String regex = secondeLevel.getRegex();
				filterMap.put(firstCode + secondeCode, regex);
			}
		}
		for (String each : urls) {
			String code=each.split("\t",2)[0];
			String url=each.split("\t",2)[1];
			if(filterMap.get(code)==null){
				System.out.println(code);
			}else{
				System.out.println(code+"::::"+url.matches(filterMap.get(code)));
			}
		}
	}

	public static class SecondeLevel {
		private String num;
		private String regex;

		public SecondeLevel() {
			super();
			// TODO Auto-generated constructor stub
		}

		public String getNum() {
			return num;
		}

		public void setNum(String num) {
			this.num = num;
		}

		public String getRegex() {
			return regex;
		}

		public void setRegex(String regex) {
			this.regex = regex;
		}

		public SecondeLevel(String num, String regex) {
			super();
			this.num = num;
			this.regex = regex;
		}

	}

	public static class FirstLevel {
		private String num;
		private SecondeLevel[] child;

		public String getNum() {
			return num;
		}

		public void setNum(String num) {
			this.num = num;
		}

		public SecondeLevel[] getChild() {
			return child;
		}

		public void setChild(SecondeLevel[] child) {
			this.child = child;
		}

		public FirstLevel() {
			super();
			// TODO Auto-generated constructor stub
		}

		public FirstLevel(String num, SecondeLevel[] child) {
			super();
			this.num = num;
			this.child = child;
		}

	}
}
