package com.kevin.jkudu.utils;

public class StringUtils {

	public static boolean isEmpty(String str){
		if (null == str || "".equals(str.trim()))
			return true;
		return false;
	}
	
}
