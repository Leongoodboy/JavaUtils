package etl_tools;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.input.ExcelXLSInputer;
import com.seassoon.utils.ConnectionDB;

public class 批量添加治理数据 {
	public static void main(String[] args) throws Exception {
		
		
		ConnectionDB connectionDB=new ConnectionDB("jdbc:mysql://10.50.5.12/db_suichao_internal?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull", "suichao_internal", "1f3KM1aG9VFcdgkS");
		
	
		
		CsvInputer input= new CsvInputer("C:\\Users\\zhangqianfeng\\Downloads\\aa.csv","UTF-8");

		input.setIsFirstHeader(false);
		input.initialize();

		String[] header = input.getHeader();

		System.out.println(Joiner.on(",").useForNull("").join(header));

		System.out.println("-----------------------------------");

		String[] data;

		while ((data = input.next()) != null) {

			if (data.length > 0) {
				System.out.println(Joiner.on(",").useForNull("").join(data));
			}
			
			Map<String, String>  map =new HashMap<>();
			
			
			map.put("process_id", "130");
		
			map.put("path", "/user/root/suichao/自贸区项目/处理数据/原始数据");
			
			
			String sql= "select * from resource where path  ='/user/root/suichao/自贸区项目/原始数据/采集库数据/"+data[0]+"'";
		
			String sql2= "select * from resource where path  ='/user/root/suichao/自贸区项目/处理数据/原始数据/"+data[0]+"'";
			
			List<Map<String, Object>> list=		connectionDB.excuteQuery(sql, null);
			
			if(connectionDB.excuteQuery(sql, null).size()==0){
				continue;
			}
//			if(list.size()>0){
//				System.out.println(list.get(0).get("id"));
//				map.put("res_id", list.get(0).get("id").toString());
//				map.put("rename", list.get(0).get("name").toString());	
//			}else{
//				continue;
//			}
	
//			String response=	HttpUtils.sendGet("http://localhost/process/process/processAddResource", map, 5000);
//			
//			
//			System.out.println(response);
			
		}
	}
}
