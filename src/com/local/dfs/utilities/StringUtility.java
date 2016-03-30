package com.local.dfs.utilities;

public class StringUtility {
	public static String getSegment(int i, String str, String delimiter) {
		String segs[] = str.split(delimiter);
		return segs[i];
	}
}
