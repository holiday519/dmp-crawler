package com.pxene.dmp.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

public class CrawlerConfig {

	private String[] seeds;
	private LoginConf loginConf;
	private ProxyConf proxyConf;
	private static Gson gson = new Gson();
	private static Map<String, CrawlerConfig> confCache = new HashMap<String, CrawlerConfig>();

	private static final String USERAGENT_LIST_PATH = "/proxy/useragent.list";
	private static final String PROXY_LIST_PATH = "/proxy/ip.list";
	private static Map<String, String[]> listCache = new HashMap<String, String[]>();

	private CrawlerConfig() {

	}

	public String[] getSeeds() {
		return seeds;
	}

	public void setSeeds(String[] seeds) {
		this.seeds = seeds;
	}

	public LoginConf getLoginConf() {
		return loginConf;
	}

	public void setLoginConf(LoginConf loginConf) {
		this.loginConf = loginConf;
	}

	public ProxyConf getProxyConf() {
		return proxyConf;
	}

	public void setProxyConf(ProxyConf proxyConf) {
		this.proxyConf = proxyConf;
	}

	public static CrawlerConfig load(String fileName) {
		if (confCache.containsKey(fileName)) {
			return confCache.get(fileName);
		}
		JsonReader reader = new JsonReader(new InputStreamReader(
				CrawlerConfig.class.getResourceAsStream(fileName)));
		CrawlerConfig config = gson.fromJson(reader, CrawlerConfig.class);
		// 设置代理
		ProxyConf proxyConf = config.getProxyConf();
		if (proxyConf.enable) {
			proxyConf.setUserAgents(getList(USERAGENT_LIST_PATH));
			proxyConf.setIps(getList(PROXY_LIST_PATH));
			config.setProxyConf(proxyConf);
		}
		confCache.put(fileName, config);
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return config;
	}

	public class LoginConf {
		private boolean enable;
		private String url;
		private String usrkey;
		private String username;
		private String pwdkey;
		private String password;

		public boolean isEnable() {
			return enable;
		}

		public void setEnable(boolean enable) {
			this.enable = enable;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public String getUsrkey() {
			return usrkey;
		}

		public void setUsrkey(String usrkey) {
			this.usrkey = usrkey;
		}

		public String getUsername() {
			return username;
		}

		public void setUsername(String username) {
			this.username = username;
		}

		public String getPwdkey() {
			return pwdkey;
		}

		public void setPwdkey(String pwdkey) {
			this.pwdkey = pwdkey;
		}

		public String getPassword() {
			return DigestUtils.md5Hex(password);
		}

		public void setPassword(String password) {
			this.password = password;
		}
	}

	public class ProxyConf {
		private boolean enable;
		private String[] ips;
		private String[] userAgents;

		public boolean isEnable() {
			return enable;
		}

		public void setEnable(boolean enable) {
			this.enable = enable;
		}

		public String[] getIps() {
			return ips;
		}

		public void setIps(String[] ips) {
			this.ips = ips;
		}

		public String randomIp() {
			return ips[new Random().nextInt(ips.length)];
		}

		public String[] getUserAgents() {
			return userAgents;
		}

		public void setUserAgents(String[] userAgents) {
			this.userAgents = userAgents;
		}

		public String randomUserAgent() {
			return userAgents[new Random().nextInt(userAgents.length)];
		}
	}

	private static String[] getList(String fileName) {
		if (listCache.containsKey(fileName)) {
			return listCache.get(fileName);
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				CrawlerConfig.class.getResourceAsStream(fileName)));
		String result = "";
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.length() != 0) {
					result += line + "|";
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result.substring(0, result.length() - 1).split("\\|");
	}

//	public static void main(String[] args) {
//		String usr = "568709012";
//		String pwd = "holiday519";
//		// 验证码
//		String vcode = "!PQE";
//		// 生成salt
//		String salt = getSalt(usr);
//		// md5
//		String pwdMd5 = DigestUtils.md5Hex("holiday519");
//		// 加密1次
//		String h1 = hexchar2bin(pwdMd5);
//		// 加密2次
//		String s2 = DigestUtils.md5Hex(h1 + salt);
//		// 处理验证码
//		String hexVcode = strToBytes(vcode.toUpperCase(), true);
//		String vcodeLen = new Integer(hexVcode.length() / 2).toString(16);
//		while (vcodeLen.length() < 4) {
//			vcodeLen = "0" + vcodeLen;
//		}
//
//	}
//
//	public static String getSalt(String usr) {
//		int maxLength = 16;
//		Integer str = Integer.parseInt(usr);
//		String hex = str.toString(16);
//		int len = hex.length();
//		for (int i = len; i < maxLength; i++) {
//			hex = "0" + hex;
//		}
//		String[] arr = new String[8];
//		int k = 0;
//		for (int j = 0; j < maxLength; j += 2) {
//			arr[k] = "\\x" + hex.substring(j, j + 2);
//			k++;
//		}
//		String result = org.apache.commons.lang.StringUtils.join(arr, "");
//		System.out.println(result);
//		return result;
//	}
//
//	public static String strToBytes(String str, boolean unicode) {
//		if (str == null) {
//			return "";
//		}
//		if (unicode) {
//			str = utf16ToUtf8(str);
//		}
//		int len = str.length();
//		char[] data = new char[len];
//		for (int i = 0; i < len; i++) {
//			data[i] = str.charAt(i);
//		}
//		return bytesInStr(data);
//	}
//
//	public static String utf16ToUtf8(String str) {
//		int len = str.length();
//		List<String> list = new ArrayList<String>();
//		for (int i = 0; i < len; i++) {
//			char code = str.charAt(i);
//			if (code > 0x0 && code <= 0x7f) {
//				list.add(String.valueOf(code));
//			} else if (code >= 0x80 && code <= 0x7ff) {
//				list.add(String.valueOf(0xc0 | ((code >> 6) & 0x1f)));
//				list.add(String.valueOf(0x80 | (code & 0x3f)));
//			} else if (code >= 0x800 && code <= 0xffff) {
//				list.add(String.valueOf(0xe0 | ((code >> 12) & 0xf)));
//				list.add(String.valueOf(0x80 | ((code >> 6) & 0x3f)));
//				list.add(String.valueOf(0x80 | (code & 0x3f)));
//			}
//		}
//		String result = org.apache.commons.lang.StringUtils.join(
//				list.toArray(), "");
//		System.out.println(result);
//		return result;
//	}
//
//	public static String bytesInStr(char[] data) {
//		if (data == null || data.length == 0) {
//			return "";
//		}
//		String outInHex = "";
//		for (int i = 0; i < data.length; i++) {
//			String hex = new Integer((int) data[i]).toString(16);
//			if (hex.length() == 1) {
//				hex = "0" + hex;
//			}
//			outInHex += hex;
//		}
//		return outInHex;
//	}
//
//	public static String hexchar2bin(String pwdMd5) {
//		int len = pwdMd5.length();
//		String[] arr = new String[len / 2];
//		int k = 0;
//		for (int i = 0; i < len; i = i + 2) {
//			arr[k] = "\\x" + pwdMd5.substring(i, i + 2);
//			k++;
//		}
//		String result = org.apache.commons.lang.StringUtils.join(arr, "");
//		System.out.println(result);
//		return result;
//	}
//
//	public static String encryptStr(String str, boolean isASCII) {
//		int[] data = dataFromStr(str, isASCII);
//
//	}
//
//	public static int[] dataFromStr(String str, boolean isASCII) {
//		int len = str.length();
//		int[] data = new int[len];
//		if (isASCII) {
//			for (int i = 0; i < len; i++) {
//				data[i] = str.charAt(i) & 0xff;
//			}
//		} else {
//			int k = 0;
//			for (int i = 0; i < len; i += 2) {
//				data[k++] = Integer.parseInt(str.substring(i, i + 2), 16);
//			}
//		}
//		return data;
//	}
//
//	public static int[] encryptChar(int[] data) {
//		long[] plain = new long[8];
//		int[] prePlain = new int[8];
//		int cryptPos = 0;
//		int preCryptPos = 0;
//		boolean header = true;
//		int pos = 0;
//		int len = data.length;
//		int padding = 0;
//		pos = (len + 0x0A) % 8;
//		if (pos != 0)
//			pos = 8 - pos;
//		int[] out = new int[len + pos + 10];
//		plain[0] = ((rand() & 0xF8) | pos) & 0xFF;
//		for (int i = 1; i <= pos; i++)
//			plain[i] = rand() & 0xFF;
//		pos++;
//		for (int i = 0; i < 8; i++)
//			prePlain[i] = 0;
//		padding = 1;
//		while (padding <= 2) {
//			if (pos < 8) {
//				plain[pos++] = rand() & 0xFF;
//				padding++;
//			}
//			if (pos == 8)
//				__encrypt8bytes();
//		}
//		int i = 0;
//		while (len > 0) {
//			if (pos < 8) {
//				plain[pos++] = data[i++];
//				len--;
//			}
//			if (pos == 8)
//				__encrypt8bytes();
//		}
//		padding = 1;
//		while (padding <= 7) {
//			if (pos < 8) {
//				plain[pos++] = 0;
//				padding++;
//			}
//			if (pos == 8)
//				__encrypt8bytes();
//		}
//		return out;
//	}
//
//	public static long rand() {
//		return Math.round(Math.random() * 0xffffffff);
//	}
}
