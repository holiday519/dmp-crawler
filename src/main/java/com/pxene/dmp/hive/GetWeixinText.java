package com.pxene.dmp.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.pxene.dmp.common.CjhCookieTool;
import com.pxene.dmp.common.UATool;
import com.pxene.dmp.constant.IPList;

public class GetWeixinText extends Thread {
	private static Log logger = LogFactory.getLog(GetWeixinText.class);
	private static Configuration conf = new Configuration();
	private static FileSystem fs;
	private String time;

	static {
		conf.set("fs.defaultFS", "hdfs://dmp01:9000");
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public GetWeixinText(String time) {
		this.time=time;
	}
	
	@Override
	public void run() {
		getPageContent(time);
	}

	public static void main(String[] args) {
		String time = "2016031416";
		String time2 = "2016031415";
		new GetWeixinText(time2).start();
		new GetWeixinText(time).start();
	}

	private static void getPageContent( String time) {
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			connection = DriverManager.getConnection("jdbc:hive2://192.168.3.151:10000/db_telecom", "root", "");
			String sql = "select imsi,biz,mid,idx,sn from ee_weixinbiz_v03 where datetime=" + time;
			statement = connection.createStatement();
			rs = statement.executeQuery(sql);
			String imsi = "";
			String biz = "";
			String mid = "";
			String idx = "";
			String sn = "";
			String baseUrl = "http://mp.weixin.qq.com/s?__biz=[biz]&mid=[mid]&idx=[idx]&sn=[sn]";
			String basePath = "/user/chenjinghui/hiveData/";

			while (rs.next()) {
				imsi = rs.getString(1);
				biz = rs.getString(2);
				mid = rs.getString(3);
				idx = rs.getString(4);
				sn = rs.getString(5);
				logger.info("********imsi********" + time);
				if (ifexist(basePath + imsi + "/" + "*/" + biz + "_" + mid + "_" + idx + "_" + sn)) {
					continue;
				} else {
					String url = baseUrl.replace("[biz]", biz).replace("[mid]", mid).replace("[idx]", idx).replace("[sn]", sn);
					logger.info("******URL*********" + url);
					Document doc = getDoc(url);
					String title = doc.title();
					String content = doc.select("div#page-content").text();
					createFile(basePath + imsi + "/" + time.substring(0, time.length()-2) + "/" + biz + "_" + mid + "_" + idx + "_" + sn, (title + "\r\n" + content).getBytes());
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// rs statement connection依次关闭
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				statement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// 根据微信号的信息匹配路径
	public static boolean ifexist(String dest) {
		long count = 0;

		try {
			FileStatus[] status = fs.globStatus(new Path(dest));
			count = status.length;
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (count > 0) {
			return true;
		}
		return false;
	}

	// 确保document一定获取成功
	private static Document getDoc(String url) {
		Document doc = null;
		int i = 0;
		while (true) {
			try {
				doc = Jsoup.connect(url).userAgent(UATool.getUA()).get();
				break;
			} catch (IOException e) {
				logger.info("抓取失败。。。重来。。。");
				List<String> iplist = IPList.elements();
				String ipstr = iplist.get(new Random().nextInt(iplist.size()));
				System.getProperties().setProperty("proxySet", "true");
				System.getProperties().setProperty("http.proxyHost", ipstr.split(":")[0]);
				System.getProperties().setProperty("http.proxyPort", ipstr.split(":")[1]);
				i++;
			}

			// 防止过多次的死循环
			if (i >= 20) {
				break;
			}

		}
		return doc;
	}

	// 创建新文件
	public static void createFile(String dst, byte[] contents) throws IOException {
		Path dstPath = new Path(dst); // 目标路径
		// 打开一个输出流
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(contents);
		outputStream.close();
		// fs.close();
	}
}
