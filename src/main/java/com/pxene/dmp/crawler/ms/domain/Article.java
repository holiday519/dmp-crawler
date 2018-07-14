package com.pxene.dmp.crawler.ms.domain;

import org.apache.solr.client.solrj.beans.Field;


/**
 * 文章实体类
 * @author Administrator
 * @Field
 *	private String time;
 */
public class Article {
	
	@Field
	private String id;
	@Field
	private String title;
	
	private String time;
	@Field
	private String content;
	
	private String author;
	private String sections;
	private String url;
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public String getSections() {
		return sections;
	}
	public void setSections(String sections) {
		this.sections = sections;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
	
}
