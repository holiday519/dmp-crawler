package com.pxene.dmp.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
		private String username;
		private String password;
		private Cookie cookie;
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
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public String getPassword() {
			return password;
		}
		public void setPassword(String password) {
			this.password = password;
		}
		public Cookie getCookie() {
			return cookie;
		}
		public void setCookie(Cookie cookie) {
			this.cookie = cookie;
		}
		public class Cookie {
			private String url;
			private String usekey;
			private String pwdkey;
			public String getUrl() {
				return url;
			}
			public void setUrl(String url) {
				this.url = url;
			}
			public String getUsekey() {
				return usekey;
			}
			public void setUsekey(String usekey) {
				this.usekey = usekey;
			}
			public String getPwdkey() {
				return pwdkey;
			}
			public void setPwdkey(String pwdkey) {
				this.pwdkey = pwdkey;
			}
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
		return result.substring(0, result.length()-1).split("\\|");
	}
}
