package com.seassoon.js;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.seassoon.utils.FileUtils;

public class JsFunction {
	/**
	 * @author:sunxinli
	 * @param x
	 *            处理的参数
	 * @param y
	 *            处理的参数
	 * @param js
	 *            处理的方式 js
	 */
	public static void main(String[] args) {
		
		Map<String, Object> valueMap = new HashMap<String, Object>();
		valueMap.put("c1", 1);
		valueMap.put("c2", "dsafdsa、3434");
		valueMap.put("c3", 3);
		
		final Map<String, Object> map = new HashMap<String, Object>();
		map.put("x", 1);
		map.put("row", new String[]{"aa","bb","cc"});
		map.put("valueMap",valueMap);
		
		System.out.println(execute(map, "valueMap.c2.split('、')[1]", "STORE_ERROR"));
		
	//	System.out.println(execute(map, "new Date(\"2013/07/01\").format('yyyy-MM-dd hh:mm:ss')", "STORE_ERROR")); 
		
		ThreadPoolExecutor threadPoolExecutor =new ThreadPoolExecutor(10, 10, 100, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		
		for (int i = 0; i < 100000; i++) {
			map.put("value",i);
			final Integer i_tmp=i;
			threadPoolExecutor.execute(new Runnable() {
				
				@Override
				public void run() {
					System.out.println(i_tmp+"_"+   execute(map, "value", "STORE_ERROR"));
					
				}
			});
		}
		
		
	}
	static ScriptEngineManager manager = new ScriptEngineManager();
	static ScriptEngine engine = manager.getEngineByName("js");
	
	static{
		try {
			 
			engine.eval("Date.prototype.format=function(format){var date={\"M+\":this.getMonth()+1,\"d+\":this.getDate(),\"h+\":this.getHours(),\"m+\":this.getMinutes(),\"s+\":this.getSeconds(),\"q+\":Math.floor((this.getMonth()+3)/3),\"S+\":this.getMilliseconds()};if(/(y+)/i.test(format)){format=format.replace(RegExp.$1,(this.getFullYear()+\"\").substr(4-RegExp.$1.length))}for(var k in date){if(new RegExp(\"(\"+k+\")\").test(format)){format=format.replace(RegExp.$1,RegExp.$1.length==1?date[k]:(\"00\"+date[k]).substr((\"\"+date[k]).length))}}return format};  ");
			engine.eval("var DateUtils = Java.type('com.seassoon.utils.DateUtils');");
			engine.eval("function formatDateNoSign(date){ return  DateUtils.format(DateUtils.parseDate(date,'yyyyMMdd'),'yyyy-MM-dd');  }");
			engine.eval("function contains(value,data,operation){var result=true;if(operation.toLowerCase()==\"and\"){for(var i in data){if(value.indexOf(data[i])==-1){result=false;break}}}else{if(operation.toLowerCase()==\"or\"){result=false;for(var i in data){if(value.indexOf(data[i])>-1){result=true;break}}}else{result=false}}return result}function notContains(value,data,operation){var result=true;if(operation.toLowerCase()==\"and\"){for(var i in data){if(value.indexOf(data[i])>-1){result=false;break}}}else{if(operation.toLowerCase()==\"or\"){result=false;for(var i in data){if(value.indexOf(data[i])==-1){result=true;break}}}else{result=false}}return result};");
			
//			try {
//				engine.eval(new FileReader( FileUtils.getPath()+ "/jsFunction.js"));
//			} catch (FileNotFoundException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static synchronized Object execute(Map<String, Object> map, String js, String onErrorType) {
	
		//js= js.replace("value","\""+  map.get("value").toString() +"\"");
//		if(js.replace("value", map.get("value"))){
//			
//		}
		
		
		for (Entry<String, Object> m : map.entrySet()) {
			engine.put(m.getKey(), m.getValue());
		}

		Object result = null;
		try {
			result = engine.eval(js).toString();
		} catch (Exception e) {

			e.printStackTrace();
			if (onErrorType != null) {

				if (onErrorType.equals("SET_TO_BLANK")) {
					return "";
				} else if (onErrorType.equals("STORE_ERROR")) {
					return e.getMessage();
				} else if (onErrorType.equals("KEEP_ORIGINAL")) {
					return map.get("value");
				}

			}
		}
		return result;
	}
}
