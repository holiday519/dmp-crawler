package com.pxene.dmp.common;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

	public static String regexpExtract(String str, String regex) {
		if (str == null || regex == null) {
			return "";
		}
		Pattern REGEX_PATTERN = Pattern.compile(regex);
		Matcher matcher = REGEX_PATTERN.matcher(str);
		if (matcher.find()) {
			return matcher.group(1).trim();
		}
		return "";
	}

	public static String removePunctuations(String str) {
		if (str == null) {
			return "";
		}
		return str.replaceAll("[\\pP\\pZ\\pS]", "");
	}
	
	public static Set<String> fullRegexpExtract(String str, String regex) {
		Set<String> set=new HashSet<>();
		if (str == null || regex == null) {
			 set.add("") ;
			 return set;
		}
		Pattern REGEX_PATTERN = Pattern.compile(regex);
		Matcher matcher = REGEX_PATTERN.matcher(str);
		while(matcher.find()){
//			System.out.println(matcher.group());
			set.add(matcher.group().trim());
		}
//		for(int i=1;i<matcher.groupCount();i++){
//			set.add(matcher.group(1).trim());
//		}
			return set;
	}

}
