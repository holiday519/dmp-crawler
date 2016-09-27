package com.pxene.dmp.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pxene.dmp.domain.Article;


/**
 * solr工具类
 *
 */
public class SolrUtil {
	static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);
	private static final String SOLR_URL = "http://115.182.33.161:8983/solr/solr_test2"; // 服务器地址
	private static HttpSolrServer server = null;
	static{
		try {
			server = new HttpSolrServer(SOLR_URL);
			server.setAllowCompression(true);
			server.setConnectionTimeout(10000);
			server.setMaxTotalConnections(100);
		} catch (Exception e) {
			logger.error("请检查tomcat服务器或端口是否开启!{}",e);
			e.printStackTrace();
		}
	}
	/**
	 * 建立索引
	 * @throws Exception
	 */
	public static void addIndex(Article article) {
		try {
			server.addBean(article);
			server.commit(true, true, true);//软提交
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 建立索引
	 * @throws Exception
	 */
	public static void deleteIndex() {
		try {
			List<String> ids = new ArrayList<String>();
			ids.add("00030006_0000001");
			ids.add("00030006_0000003");
			ids.add("00030006_0000005");
			ids.add("00030006_0000002");
			ids.add("00030006_0000004");
			server.deleteById(ids);
			server.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 建立索引
	 * @throws Exception
	 */
	public static void deleteAll() {
		try {
			server.deleteByQuery("*:*");  
            server.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 查询
	 * @param skey 
	 * @param row 
	 * @param start 
	 * @param sort 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static Map<String,Object> search(String skey, Long start, Long row) throws Exception {
		Map<String,Object> hashMap = new HashMap<String, Object>();
		
		SolrQuery params = new SolrQuery();
		params.setQuery("text:"+skey);
		//设置分页
		params.setStart(start.intValue());
		params.setRows(row.intValue());
		
		//设置高亮
		params.setHighlight(true);
		params.addHighlightField("title");
		params.addHighlightField("describe");
		params.setHighlightSimplePre("<font color='red'>");
		params.setHighlightSimplePost("</font>");
		
		QueryResponse response = server.query(params);
		SolrDocumentList results = response.getResults();
		long numFound = results.getNumFound();
		hashMap.put("numFound", numFound);
		Map<String, Map<String, List<String>>> highlighting = response.getHighlighting();
		List<Article> beans = response.getBeans(Article.class);
		for (Article article : beans) {
			Map<String, List<String>> map = highlighting.get(article.getId());
			
			List<String> title_list = map.get("title");
			if(title_list!=null && title_list.size()>0){
				article.setTitle(title_list.get(0));
			}
			
			List<String> desc_list = map.get("describe");
			if(desc_list!=null && desc_list.size()>0){
//				article.setDescribe(desc_list.get(0));
			}
		}
		hashMap.put("dataList", beans);
		return hashMap;
	}
	
	public static void main(String[] args) {
		SolrUtil.deleteAll();
	}
	
	
	
}
