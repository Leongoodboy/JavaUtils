package etl_tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;


import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.collect.Multiset.Entry;
import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.utils.JsonUtils;

import net.sf.json.JSONObject;

public class Test {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		CsvInputer csvInput= new CsvInputer("E:\\思贤\\工作文件夹\\江西高速测试数据\\ODS\\GLJCK\\RoadListGS.csv","gbk");
		
//		csvInput.initialize();
//		
//		csvInput.setIgnoreLines(2);
//		csvInput.setHeaderLines(1);
		
		String[] header = csvInput.getHeader();
		
		System.out.println(Joiner.on(",").useForNull("").join(header));
		
		System.out.println("-----------------------------------");
		
		
		List<String> codeList=new ArrayList<>();
		
		String[] data;
		
		while ((data = csvInput.next()) != null) {

			if(data.length>0){
				System.out.println(data[1]+data[2]);
				codeList.add(data[1]);
			}
		
			
		}
		
		
		
		JsonNode rootNode = JsonUtils.MAPPER.readTree(IOUtils.toString(new FileReader("C:\\Users\\zhangqianfeng\\Documents\\Tencent Files\\1539043957\\FileRecv\\bridge.json")));
		
		//JSONObject jsonObject = JSONObject.fromObject(IOUtils.toString(new FileReader("C:\\Users\\zhangqianfeng\\Documents\\Tencent Files\\1539043957\\FileRecv\\bridge.json")));
		
//		Map  map= JsonUtils.MAPPER.readValue(new File("C:\\Users\\zhangqianfeng\\Documents\\Tencent Files\\1539043957\\FileRecv\\bridge.json"), Map.class);
		 
//		System.out.println(map.get("features"));
		
		
		List<JsonNode>  list  =new ArrayList<>();
		
		for (JsonNode node : rootNode.get("features")) {
			
			
			System.out.println(node.get("properties").get("ROADBM"));
			String gsCode= node.get("properties").get("ROADBM").asText().toString();
			
			if(gsCode.indexOf("36")>=0){
				if( codeList.contains(gsCode.substring(0,gsCode.indexOf("36") ))){
//					if(list.size() <10){
						list.add(node);	
//					}
				
				}
			}else{
				System.out.println(gsCode);
			}
		
			
		
			
		}
		
		
		Map outMap =new LinkedHashMap<>();
		
		outMap.put("type", "FeatureCollection");
		outMap.put("features", list);
		
		
		
//	System.out.println(  JsonUtils.MAPPER.writeValueAsString(outMap));
		
		IOUtils.write( JsonUtils.MAPPER.writeValueAsBytes(outMap), new FileOutputStream("C:\\Users\\zhangqianfeng\\Documents\\Tencent Files\\1539043957\\FileRecv\\bridge2.json"));
	
	}

}
