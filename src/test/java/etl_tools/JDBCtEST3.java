package etl_tools;

import com.seassoon.etl_tools.input.JDBCInputer;
import com.seassoon.etl_tools.outout.CsvOutputer;

public class JDBCtEST3 {

	public static void main(String[] args) throws Exception {
//		JDBCInputer inputer = new JDBCInputer("select profile_data from sc_field_profile",
//
//				"jdbc:mysql://172.16.40.8:3306/db_suichao?useUnicode=true&characterEncoding=utf8", "root", "sxad!#%&(24680",
//				"mysql");
		JDBCInputer inputer = new JDBCInputer("select * from TB_ZXSJJH_WENSHU",

				"jdbc:oracle:thin:@192.168.222.75:1521:ORCL", "SJZX", "Test",
				"Oracle");

		inputer.initialize();
		
		CsvOutputer csvOutputer =new CsvOutputer("C:\\Users\\zhangqianfeng\\Downloads\\TB_ZXSJJH_WENSHU.csv", inputer.getHeader());
		
		csvOutputer.initialize();
		
	
		
		
		inputer.out(csvOutputer);	
	}
}
