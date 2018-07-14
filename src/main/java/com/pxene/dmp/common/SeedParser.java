package com.pxene.dmp.common;

public class SeedParser {

	public static String[] parse(String seed) {
		if (seed.matches(".*\\[.+\\].*")) {
			String range = StringUtils.regexpExtract(seed, "\\[(.+)\\]");
			// 数字
			if (range.matches("[0-9\\-]+")) {
				int start = Integer.parseInt(range.split("-")[0]);
				int end = Integer.parseInt(range.split("-")[1]);
				String[] seeds = new String[end-start+1];
				for (int i=0; i<seeds.length; i++) {
					seeds[i] = seed.replaceAll("\\[(.+)\\]", Integer.toString(start+i));
				}
				return seeds;
			}
			// 字母
			if (range.matches("[a-zA-Z\\-]+")) {
				// ascii
				int start = (int)range.split("-")[0].toCharArray()[0];
				int end = (int)range.split("-")[1].toCharArray()[0];
				String[] seeds = new String[end-start+1];
				for (int i=0; i<seeds.length; i++) {
					seeds[i] = seed.replaceAll("\\[(.+)\\]", Character.toString((char)(start+i)));
				}
				return seeds;
			}
		}
		return new String[] {seed};
	}
	
}
