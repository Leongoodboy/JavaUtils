package etl_tools;

import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.outout.MongoDBOutputer;

public class Csv_mongo {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		CsvInputer csvInput= new CsvInputer("C:\\Users\\zhangqianfeng\\Desktop\\财产\\财产_银行存款.csv","UTF-8");
		
		csvInput.initialize();
		
	
		
		
		
		MongoDBOutputer outputer =new MongoDBOutputer("gzfy", "yinhangcunkuan", "10.50.5.14", 28117, csvInput.getHeader());
		
		outputer.initialize();
		
		 csvInput.out(outputer); 
		
//		String[] header = csvInput.getHeader();
//		
//		System.out.println(Joiner.on(",").useForNull("").join(header));
//		
//		System.out.println("-----------------------------------");
//		
		
//		System.out.println(Joiner.on(",").useForNull("").join(inputer.getHeader()));
//		String[] data;
//		
//		while ((data = inputer.next()) != null) {
//
//			if(data.length>0){ 
//				System.out.println(Joiner.on(",").useForNull("").join(data));
//			}
//		
//			
//		}
		
	}

}
