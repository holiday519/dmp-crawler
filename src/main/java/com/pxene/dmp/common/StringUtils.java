package com.pxene.dmp.common;

import java.text.SimpleDateFormat;
import java.util.Date;
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
	
	public static String removeLineBreak(String str) {
        if (str == null) {
            return "";
        }
        return str.replaceAll("[\\n\\r]", "");
    }
	
	/**
	 * 时间戳转换成日期格式字符串
	 * 
	 * @param seconds
	 *            精确到秒的字符串
	 * @param formatStr
	 * @return
	 */
	public static String timeStamp2Date(String seconds, String format) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}

	/**
	 * 日期格式字符串转换成时间戳
	 * 
	 * @param date
	 *            字符串日期
	 * @param format
	 *            如：yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String date2TimeStamp(String date_str, String format) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return String.valueOf(sdf.parse(date_str).getTime() / 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	/**
	 * 在原有的字符串上插入一个新的字符串
	 * @param original
	 * @param insert
	 * @param index
	 * @return
	 */
	public static String stringinsert(String original,String insert,int index){     
	    return original.substring(0,index)+insert+original.substring(index,original.length());
	} 

}
