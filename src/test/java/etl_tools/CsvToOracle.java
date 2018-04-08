package etl_tools;

import java.io.FileNotFoundException;

import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.outout.JDBCOutputer;

public class CsvToOracle {

	public static void main(String[] args) throws Exception {
		

		CsvInputer csvInput = new CsvInputer("E:\\思贤\\工作文件夹\\贵州高院\\导出数据\\导出数据\\TB_ZXSJJH_AJXX.csv", "UTF-8");

	
		
		JDBCOutputer jdbcOutputer =new JDBCOutputer("TB_ZXSJJH_AJXX",csvInput.getHeader(),"jdbc:oracle:thin:@10.50.5.42:1521:ORCL", "SJZX2", "123456",
				"oracle.jdbc.driver.OracleDriver");
		
		csvInput.out(jdbcOutputer);
		
		
		
	}

}
