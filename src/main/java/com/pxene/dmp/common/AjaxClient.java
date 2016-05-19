package com.pxene.dmp.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AjaxClient {
	
	private final CloseableHttpClient client = HttpClients.createDefault();
	private final HttpGet request;
	private static final Gson GSON = new Gson();
	
	public static class Builder {
		private String url;
		private Map<String, String> headers;
		
		public Builder(String url, Map<String, String> headers) {
			this.url = url;
			this.headers = headers;
		}
		public Builder setUrl(String url) {
			this.url = url;
			return this;
		}
		public Builder setHeaders(Map<String, String> headers) {
			this.headers = headers;
			return this;
		}
		
		public AjaxClient build() {
			return new AjaxClient(this);
		}
	}
	
	private AjaxClient(Builder builder) {
		String url = (builder.url == null) ? "" : builder.url;
		Map<String, String> headers = (builder.headers == null) ? new HashMap<String, String>() : builder.headers;
		request = new HttpGet(url);
		for (Map.Entry<String, String> entry : headers.entrySet()) {
			request.setHeader(entry.getKey(), entry.getValue());
		}
	}
	
	public JsonObject execute() {
		HttpResponse resp = null;
		try {
			resp = client.execute(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return getResult(resp);
	}
	
	private JsonObject getResult(HttpResponse resp) {
		byte[] cache = new byte[512];
		try {
			InputStream input = resp.getEntity().getContent();
			input.read(cache);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return GSON.fromJson(new String(cache, 0, cache.length).trim(), JsonObject.class);
	}
}
