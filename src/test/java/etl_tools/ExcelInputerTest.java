package etl_tools;

import com.google.common.base.Joiner;
import com.seassoon.etl_tools.input.ExcelXLSInputer;

public class ExcelInputerTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		ExcelXLSInputer input = new ExcelXLSInputer("C:\\Users\\zhangqianfeng\\Downloads\\环境影响评价信息.xls");

		// CsvInput csvInput= new
		// CsvInput("C:\\Users\\zhangqianfeng\\Downloads\\2016年1-5月份临时救助总表
		// (1).csv","gbk");

//		input.setIgnoreLines(0).setSkipDataLines(0);
//		input.setHeaderLines(1);

		input.initialize();

		String[] header = input.getHeader();

		System.out.println(Joiner.on(",").useForNull("").join(header));

		System.out.println("-----------------------------------");

		String[] data;

		while ((data = input.next()) != null) {

			if (data.length > 0) {
				System.out.println(Joiner.on(",").useForNull("").join(data));
			}

		}

	}

}
