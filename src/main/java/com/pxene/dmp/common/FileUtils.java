package com.pxene.dmp.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;


/**
 * 
 * @author John
 *
 */
public class FileUtils {
	
	/**
	 * 写文件
	 * @param filePath
	 * @param content
	 */
	public static void writeFile(String filePath,String content){
		//1.创建源
		File file = new File(filePath);
		//2.选择流
		OutputStream os = null;
		BufferedWriter bw = null;
		try {
			os = new FileOutputStream(file,true);
			//3.操作
			os.write((content+"\r\n").getBytes());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 读取文件
	 * @param filePath
	 */
	public static void readAndWriteFile(String inFilePath,String outFilePath){
		//解码
		try {
			File file = new File(outFilePath);
			OutputStream os = null;
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inFilePath),"UTF-8"));
			String line = null;
			os = new FileOutputStream(file,true);
			Integer num = 70001;
			while(null != (line = reader.readLine())){
				int length = num.toString().length();
				String content_pref = "";
				if(length==1){//num为个位数
					content_pref = "00000";
				}else if(length==2){//num为十位数
					content_pref = "0000";
				}else if(length==3){//num为百位数
					content_pref = "000";
				}else if(length==4){//num为千位数
					content_pref = "00";
				}else if(length==5){//num为万位数
					content_pref = "0";
				}
				String content = "00480594002_" + content_pref + num + "=" + line;
				os.write((content+"\r\n").getBytes());
				num += 1;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		readAndWriteFile("C:/Users/zhuzi/Desktop/cec/fjbk8.txt","C:/Users/zhuzi/Desktop/cec/fjbk.properties");
	}
}
