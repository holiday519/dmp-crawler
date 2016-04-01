package com.pxene.dmp.common;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class URLUtils {

	public static String regexpExtract(String url, String regex) {
		if (url == null || regex == null) {
			return "";
		}
		Pattern REGEX_PATTERN = Pattern.compile(regex);
		Matcher matcher = REGEX_PATTERN.matcher(url);
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

	public static String getRquestContent(String url) {
		String result = "";
		// 构造HttpClient的实例
		HttpClient httpClient = new HttpClient();
		// 创建GET方法的实例
		GetMethod getMethod = new GetMethod(url);
		// 使用系统提供的默认的恢复策略
		getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
		try {
			// 执行getMethod
			int statusCode = httpClient.executeMethod(getMethod);
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + getMethod.getStatusLine());
			}
			// 读取内容
			byte[] responseBody = getMethod.getResponseBody();
			// 处理内容
			result = new String(responseBody, "gbk");
			// System.out.println(new String(responseBody, "gbk"));
		} catch (HttpException e) {
			// 发生致命的异常，可能是协议不对或者返回的内容有问题
			System.out.println("Please check your provided http address!");
			e.printStackTrace();
		} catch (IOException e) {
			// 发生网络异常
			e.printStackTrace();
		} finally {
			// 释放连接
			getMethod.releaseConnection();
		}
		return result;
	}

}
