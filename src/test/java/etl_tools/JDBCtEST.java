package etl_tools;

import com.seassoon.etl_tools.input.JDBCInputer;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JDBCtEST {

	public static void main(String[] args) throws Exception {
//		JDBCInputer inputer = new JDBCInputer("select profile_data from sc_field_profile",
//
//				"jdbc:mysql://172.16.40.8:3306/db_suichao?useUnicode=true&characterEncoding=utf8", "root", "sxad!#%&(24680",
//				"mysql");
		
//		JDBCInputer inputer = new JDBCInputer("select * from TB_ZXSJJH_WENSHU",
//
//				"jdbc:oracle:thin:@192.168.222.75:1521:ORCL", "SJZX", "Test",
//				"Oracle");
		

		JDBCInputer inputer = new JDBCInputer("select * from student",

				"jdbc:oracle:thin:@localhost:1521:ORCL", "monitor", "monitor",
				"Oracle");
		
		inputer.initialize();
//		JDBCOutputer jdbcOutputer = new JDBCOutputer("sc_resource_copy", inputer.getHeader(), "jdbc:mysql://localhost:3306/test2?useUnicode=true&characterEncoding=utf8&useSSL=true", "root", "123456", JDBC_DRIVER.MYSQL.getValue());
//		jdbcOutputer.initialize();

		String[] data;
		
		Long totalCount =0L;
		Long valuedCount=0L;
		Long unvaluedCount=0L;
				
		int num=0;
		while ((data = inputer.next()) != null) {

			num++;
			
			if (data.length > 0) {
				StringBuffer sb= new StringBuffer();
				for (int i = 0; i < data.length; i++) {
					sb.append("|"+data[i]);
				}
				
				System.out.println("num="+num+","+sb.toString());
				
//				JSONObject jsonObject = JSONObject.fromObject(data[0]);
//				//JsonNode node =JsonUtils.MAPPER.readTree(data[0]).get("cardinality");
//				
//				totalCount+=jsonObject.getLong("rowCount");
//				valuedCount+=jsonObject.getLong("rowCount")-jsonObject.getLong("nullCount");
//				
//				unvaluedCount+=jsonObject.getLong("nullCount");
//				
//				JSONArray jsonArray  =jsonObject.getJSONArray("topK");
//				
//				for (int i = 0; i < jsonArray.size(); i++) {
//					JSONObject object = jsonArray.getJSONObject(i);
//					if(object.getString("item").equals("0")){
//						unvaluedCount+=object.getLong("count")-object.getLong("error");
//					}else{
//						try {
//							if(object.getDouble("item") ==0){
//								unvaluedCount+=object.getLong("count")-object.getLong("error");
//							}
//							
//						} catch (Exception e) {
//							// TODO: handle exception
//						}
//					} 
//				}
				
				
				//System.out.println(Joiner.on(",").useForNull("").join(data));
			}
			
//			jdbcOutputer.process(data);

		}
		System.out.println("totalCount:"+totalCount);
		System.out.println("valuedCount:"+valuedCount);
		System.out.println("unvaluedCount:"+unvaluedCount);
		
		System.out.println("unvaluedCount:"+valuedCount*1D/totalCount*1D);
		System.out.println("unvaluedCount/totalCount:"+unvaluedCount*1D/totalCount*1D);
		
		
//		jdbcOutputer.close();
	}
}
