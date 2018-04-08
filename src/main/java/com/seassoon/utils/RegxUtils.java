package com.seassoon.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

/**
 * 正则匹配
 * @author zhangqianfeng
 *
 */
public class RegxUtils {

	
	public static String getMatcher(String regex, String source) {
		String result = "";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(source);
		while (matcher.find()) {
			
			result = matcher.group(1);
		}
		return result;
	}
	
	public static String getMatcherGroup(String regex, String source) {
		String result = "";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(source);
		while (matcher.find()) {
			
			result = matcher.group();
		}
		
		return result;
	}
	public static void main(String[] args) {
		
		String result = "";
		Pattern pattern = Pattern.compile("^(?!0,0$)");
		Matcher matcher = pattern.matcher("0,1");
		while (matcher.find()) {
			
			result = matcher.group(1);
			System.out.println(result);
		}
		//return result;
		
		//System.out.println(getMatcher("(\\d+(\\.\\d+)?)元$", "1.1元"));
	}
	
}
