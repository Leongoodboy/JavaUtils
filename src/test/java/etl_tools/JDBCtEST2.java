package etl_tools;

import com.seassoon.etl_tools.input.JDBCInputer;
import com.seassoon.etl_tools.outout.CsvOutputer;
import com.seassoon.etl_tools.outout.JDBCOutputer;

public class JDBCtEST2 {

	public static void main(String[] args) throws Exception {
//		JDBCInputer inputer = new JDBCInputer("select profile_data from sc_field_profile",
//
//				"jdbc:mysql://172.16.40.8:3306/db_suichao?useUnicode=true&characterEncoding=utf8", "root", "sxad!#%&(24680",
//				"mysql");
		
//		JDBCInputer inputer = new JDBCInputer("select * from T_TASKINFO_BAK",
//
//				"jdbc:oracle:thin:@202.120.58.119:27770:ORCL", "CITYGRID", "sxad1357924680",
//				"Oracle");
		
	
//		JDBCInputer inputer = new JDBCInputer("select * from student",
//
//				"jdbc:oracle:thin:@localhost:1521:ORCL", "monitor", "monitor",
//				"Oracle");
		
		
//		JDBCInputer inputer = new JDBCInputer("select * from T_TASKINFO",
//
//				"jdbc:oracle:thin:@10.50.5.2:1521:ORCL", "CITYGRID", "sxad1357924680",
//				"Oracle");
		

		
		
		JDBCInputer inputer = new JDBCInputer("select * from T_TASKINFO",

				"jdbc:oracle:thin:@202.120.58.119:27770:ORCL", "CITYGRID", "sxad1357924680",
				"Oracle");
		 
		 
		inputer.initialize();
		
		JDBCOutputer outputer =new JDBCOutputer("T_TASKINFO", inputer.getHeader(), "jdbc:mysql://10.50.5.12:3306/db_temp_wg?useUnicode=true&characterEncoding=utf8", "root", "1qazxsw2");
	
		outputer.setSubmitSize(1000);
		
		inputer.out(outputer);	
		
	}
}
