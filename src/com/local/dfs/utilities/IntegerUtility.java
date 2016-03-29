package com.local.dfs.utilities;

public class IntegerUtility {

	public static byte[] int2byte(int[] src) {
		int srcLength = src.length;
		byte[] res = new byte[srcLength];

		for (int i = 0; i < srcLength; i++) {
		    	res[i] = (byte) src[i];
		}

		return res;
	}
}
