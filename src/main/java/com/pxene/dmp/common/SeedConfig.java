package com.pxene.dmp.common;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

public class SeedConfig {

	private static Gson gson = new Gson();
	private static JsonObject config = gson.fromJson(new JsonReader(new InputStreamReader(SeedConfig.class
			.getResourceAsStream("/seedConfig.json"))), JsonObject.class);
	
	public static List<String> getSeeds(String key) {
		List<String> seeds = new ArrayList<String>();
		Iterator<JsonElement> iter = config.get(key).getAsJsonArray().iterator();
		while (iter.hasNext()) {
			seeds.add(iter.next().getAsString());
		}
		return seeds;
	}
	
}
