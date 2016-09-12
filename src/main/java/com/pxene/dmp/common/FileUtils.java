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
 * �ļ�������
 * @author John
 *
 */
public class FileUtils {
	
	/**
	 * д�ļ�
	 * @param filePath
	 * @param content
	 */
	public static void writeFile(String filePath,String content){
		//1.����Դ
		File file = new File(filePath);
		//2.ѡ����
		OutputStream os = null;
		BufferedWriter bw = null;
		try {
			os = new FileOutputStream(file,true);
			//3.����
			os.write((content+"\r\n").getBytes());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * ��ȡ�ļ�
	 * @param filePath
	 */
	public static void readAndWriteFile(String inFilePath,String outFilePath){
		//����
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
				if(length==1){//numΪ��λ��
					content_pref = "00000";
				}else if(length==2){//numΪʮλ��
					content_pref = "0000";
				}else if(length==3){//numΪ��λ��
					content_pref = "000";
				}else if(length==4){//numΪǧλ��
					content_pref = "00";
				}else if(length==5){//numΪ��λ��
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
