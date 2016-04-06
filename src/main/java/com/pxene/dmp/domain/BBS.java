package com.pxene.dmp.domain;

/**
 * 单个帖子，用来转化json串
 * 
 * @author xuliuming
 *
 */
public class BBS {
	private String topicid;// 帖子id
	private String replys;// 回复数
	private String views;// 点击数

	public String getTopicid() {
		return topicid;
	}

	public void setTopicid(String topicid) {
		this.topicid = topicid;
	}

	public String getReplys() {
		return replys;
	}

	public void setReplys(String replys) {
		this.replys = replys;
	}

	public String getViews() {
		return views;
	}

	public void setViews(String views) {
		this.views = views;
	}
}
