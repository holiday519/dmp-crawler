package com.pxene.dmp.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ProxyTool {
	private static  List<String> ips=new ArrayList<>();
	
	static{
		ips.add("1.63.251.103:9000");
		ips.add("112.64.28.65:8090");
		ips.add("113.80.132.16:8090");
		ips.add("59.62.6.136:9000");
		ips.add("59.38.52.165:8090");
		ips.add("27.42.160.12:8090");
		ips.add("175.184.166.86:8090");
		ips.add("119.7.93.1:8090");
		ips.add("27.12.40.180:8090");
		ips.add("119.7.91.9:9000");
	}
	
	public static Map<String, String> getIpInfo(){
		Map<String, String> ipMaps=new HashMap<>();
		Random random=new Random();
		String ip=ips.get(random.nextInt(ips.size()));
		ipMaps.put("ip", ip.split(":")[0]);
		ipMaps.put("port", ip.split(":")[1]);
		return ipMaps;
	}
	
	public static void main(String[] args) {
		System.out.println(getIpInfo().get("ip"));
		System.out.println(getIpInfo().get("port"));
	}
}
