package com.pxene.dmp.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AjaxClient {
	
	private final CloseableHttpClient client = HttpClients.createDefault();
	private final HttpGet request;
	private static final Gson GSON = new Gson();
	
	public static class Builder {
		private String url;
		private Map<String, String> headers;
		private String proxy;
		
		public Builder(String url, Map<String, String> headers) {
			this.url = url;
			this.headers = headers;
		}
		public Builder(String url, Map<String, String> headers, String proxy) {
            this.url = url;
            this.headers = headers;
            this.proxy = proxy;
        }
		public Builder setUrl(String url) {
			this.url = url;
			return this;
		}
		public Builder setHeaders(Map<String, String> headers) {
			this.headers = headers;
			return this;
		}
		public Builder setProxy(String proxy) {
		    this.proxy = proxy;
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
		request.setConfig(RequestConfig.custom().setSocketTimeout(20000).setConnectTimeout(20000).build());
		if (builder.proxy != null && builder.proxy.contains(":"))
		{
		    String[] tmpArray = builder.proxy.split(":");
		    String host = tmpArray[0];
		    int port = Integer.parseInt(tmpArray[1]);
            request.setConfig(RequestConfig.custom().setProxy(new HttpHost(host, port)).build());
		}
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
	
    public String queryJSONArrayStr()
    {
        HttpResponse resp = null;
        try
        {
            resp = client.execute(request);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return getJsonArrayResult(resp);
    }
	
	public void close() {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private JsonObject getResult(HttpResponse resp) {
		HttpEntity entity = resp.getEntity();
		String json = "";
		try {
			json = EntityUtils.toString(entity, "GBK");
		} catch (Exception exception) {
			exception.printStackTrace();
			return new JsonObject();
		}
		return GSON.fromJson(json, JsonObject.class);
	}
	
    private String getJsonArrayResult(HttpResponse resp)
    {
        if (resp != null)
        {
            HttpEntity entity = resp.getEntity();
            String json = "";
            
            try
            {
                json = EntityUtils.toString(entity, "GBK");
            }
            catch (Exception exception)
            {
                exception.printStackTrace();
                return "";
            }
            return json;
        }
        return null;
    }
}
