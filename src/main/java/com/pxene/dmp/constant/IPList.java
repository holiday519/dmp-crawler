package com.pxene.dmp.constant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IPList {
	
	private static final String FILE_NAME = "iplist.txt";

	private static Set<String> ips = new HashSet<>();
	
	static {
		File file = new File(System.getProperty("user.dir") + "/" + FILE_NAME);
		if (file.exists()) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				String line = null;
				while ((line = br.readLine()) != null) {
					ips.add(line);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static List<String> elements() {
		List<String> ipList=new ArrayList<>();
		 ipList.addAll(ips);
		 return ipList;
	}
	
	
}
