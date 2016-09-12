package com.pxene.dmp.common;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

public class ConfigUtil {

	private static final Properties PROPERTIES = new Properties();

	public static Properties loadConfigFileByPath(String path) {
		InputStreamReader inputStream = null;
		try {
			 inputStream = new InputStreamReader(ConfigUtil.class.getResourceAsStream(path),"UTF-8");
			PROPERTIES.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return PROPERTIES;
	}

	public static String getByKey(String key) {
		return PROPERTIES.getProperty(key);
	}

	/**
	 * 获取整个properties对象
	 * 
	 * @return
	 */
	public static Map getProperties() {
		return PROPERTIES;
	}

}
