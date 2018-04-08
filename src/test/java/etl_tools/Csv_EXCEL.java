package etl_tools;

import com.google.common.base.Joiner;
import com.seassoon.etl_tools.input.CsvInputer;

public class Csv_EXCEL {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		CsvInputer csvInput= new CsvInputer("C:\\Users\\zhangqianfeng\\Downloads\\part-m-00000 (26)","UTF-8");
		csvInput.setIsFirstHeader(false);

//		CsvDistributedInputer csvInput= new CsvDistributedInputer("/Users/chimney/Coding Practice/test.csv","utf-8");
//		CsvDistributedInputer csvInput= new CsvDistributedInputer(new FileInputStream("E:\\思贤\\工作文件夹\\江西高速测试数据\\ODS\\SF\\ODS_HWT_GSJTDATA201301.csv"),"UTF-8");
		//csvInput.setLimit(10);
//		CsvDistributedInputer csvInput= new CsvDistributedInputer("C:\\Users\\zhangqianfeng\\Downloads\\alcdiffdtl.csv","gbk");
		
//		csvInput.setSkipBytes(10);
		 
		
		 
//		CsvOutputer csvOutputer =new CsvOutputer("C:\\Users\\zhangqianfeng\\Downloads\\alcdiffdtl_output.csv", csvInput.getHeader());
//		
//
//		
//
//		csvInput.out(csvOutputer);
		
//		
//		csvInput= new CsvInputer("C:\\Users\\zhangqianfeng\\Downloads\\alcdiffdtl.csv","gbk");
//		
//		csvInput.initialize();
//		
//		ExcelXLSOutputer excelXLSOutputer =new ExcelXLSOutputer("C:\\Users\\zhangqianfeng\\Downloads\\环境影响评价信息3.xls",csvInput.getHeader());
//		
//		excelXLSOutputer.initialize();
//		csvInput.out(excelXLSOutputer);
		
	
		String[] header = csvInput.getHeader();
		
		System.out.println(Joiner.on(",").useForNull("").join(header));
		
		System.out.println("-----------------------------------");
		
		String[] data;
		
		while ((data = csvInput.next()) != null) {

//			if(data.length>0){
//				System.out.println(csvInput.getRecordNumer() +" "+Joiner.on(",").useForNull("").join(data));
//			}		
			
			System.out.println(csvInput.getRecordNumer() +" "+data[1]);
			
		}
		
	}

}
