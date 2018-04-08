package api;

import com.seassoon.etl.database.DataBaseFactory;
import com.seassoon.etl.database.DatabaseInterface;
import com.seassoon.etl.database.DataBaseFactory.DataBaseTypeEnum;
import com.seassoon.etl_tools.outout.JDBCOutputer;

public class Test3 {

	public static void main(String[] args) {

		/**
		 * 初始化数据库对象
		 */
		DatabaseInterface databaseInterface1 = DataBaseFactory.getDatabaseInterface(DataBaseTypeEnum.Oracle.toString(),
				"202.120.58.119", "ORCL", 27770, "CITYGRID", "sxad1357924680");
		// //oralce 11g
//		DatabaseInterface databaseInterface1 = DataBaseFactory.getDatabaseInterface(DataBaseTypeEnum.Oracle.toString(),
//				"202.120.58.119", "ORCL", 27770, "QUERYCITY", "Ws00350425");
		System.out.println(databaseInterface1.getTables());
		System.out.println(databaseInterface1.getFields("DIC_ZXLCJD"));
		//

		// //oracle 10g
//		DatabaseInterface databaseInterface2 = DataBaseFactory.getDatabaseInterface(DataBaseTypeEnum.Oracle.toString(),
//				"localhost", "orcl", 1521, "monitor", "monitor");
//		System.out.println(databaseInterface2.getTables());
//
//		System.out.println(databaseInterface2.getFields("STUDENT"));
		//

		// sqlserver 2005

//		DatabaseInterface databaseInterface3 = DataBaseFactory.getDatabaseInterface(
//				DataBaseTypeEnum.SQLServer.toString(), "192.168.2.193", "syncTest", 1433, "sa", "x123456");
//
//		System.out.println(databaseInterface3.getTables());
//
//		System.out.println(databaseInterface3.getFields("test"));

		// JDBCOutputer jdbcOutputer =new
		// JDBCOutputer("TB_ZXSJJH_AJXX",csvInput.getHeader(),"jdbc:oracle:thin:@10.50.5.42:1521:ORCL",
		// "SJZX2", "123456",
		// "oracle.jdbc.driver.OracleDriver");

	}

}
