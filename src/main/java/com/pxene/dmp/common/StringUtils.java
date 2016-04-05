package com.pxene.dmp.common;

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
			return matcher.group(1);
		}
		return "";
	}

	public static String removePunctuations(String str) {
		if (str == null) {
			return "";
		}
		return str.replaceAll("[\\pP\\pZ\\pS]", "");
	}

}
