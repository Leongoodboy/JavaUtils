package com.seassoon.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.sf.json.util.JSONUtils;


public class JsonUtils {
	
	/**
	 * 默认
	 */
	public static final ObjectMapper MAPPER = new ObjectMapper();
	/**
	 * 值为空时不参加序列化
	 */
	public static final ObjectMapper MAPPER_NON_NULL = new ObjectMapper();
	/**
	 * 忽略不存在的属性
	 */
	public static final ObjectMapper MAPPER_FAIL_ON_UNKNOWN_PROPERTIES = new ObjectMapper();
	
	
	static{
		
		MAPPER.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
		
		MAPPER_NON_NULL.setSerializationInclusion(Include.NON_NULL);
		
		MAPPER_FAIL_ON_UNKNOWN_PROPERTIES.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		
	}
	
	public static String  writeValueAsString__attrsEntrySet(Set<Entry<String, Object>> set) throws JsonProcessingException{
		
		Map<String,Object>   map =new HashMap<>();
		
		for (Entry<String, Object> entry : set) {
			map.put(entry.getKey(), entry.getValue());
		}
		
		return MAPPER.writeValueAsString(map);
		
	}
	
	
	
}

