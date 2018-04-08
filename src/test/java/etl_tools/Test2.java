package etl_tools;

import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.outout.CsvOutputer;
import com.seassoon.etl_tools.outout.ExcelXLSOutputer;

public class Test2 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		CsvInputer csvInput= new CsvInputer("C:\\Users\\zhangqianfeng\\Downloads\\alcdiffdtl.csv","gbk");
		
		csvInput.initialize();
		
		
//		CsvOutputer csvOutputer =new CsvOutputer("C:\\Users\\zhangqianfeng\\Downloads\\alcdiffdtl_output.csv", csvInput.getHeader());
//		
//		csvOutputer.initialize();
		
	
		ExcelXLSOutputer csvOutputer =new ExcelXLSOutputer("C:\\Users\\zhangqianfeng\\Downloads\\alcdiffdtl_output.csv", csvInput.getHeader());
		
		csvInput.out(csvOutputer);
		
		
		
		
		
		
		
		
//		String[] header = csvInput.getHeader();
//		
//		System.out.println(Joiner.on(",").useForNull("").join(header));
//		
//		System.out.println("-----------------------------------");
//		
//		String[] data;
//		
//		while ((data = csvInput.next()) != null) {
//
//			if(data.length>0){
//				System.out.println(Joiner.on(",").useForNull("").join(data));
//			}
//		
//			
//		}
		
	}

}
