package etl_tools;

import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.input.JDBCInputer;
import com.seassoon.etl_tools.outout.MongoDBOutputer;

public class jdbc_mongo3 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		JDBCInputer inputer = new JDBCInputer("select * from 亲属关系",

				"jdbc:mysql://localhost:3306/test3?useUnicode=true&characterEncoding=utf8", "root",
				"123456", "mysql");
		
		

		MongoDBOutputer outputer = new MongoDBOutputer("gzfy", "qinshu", "10.50.5.14", 28117, inputer.getHeader());

		outputer.initialize();

		inputer.out(outputer);

		// String[] header = csvInput.getHeader();
		//
		// System.out.println(Joiner.on(",").useForNull("").join(header));
		//
		// System.out.println("-----------------------------------");
		//

		// System.out.println(Joiner.on(",").useForNull("").join(inputer.getHeader()));
		// String[] data;
		//
		// while ((data = inputer.next()) != null) {
		//
		// if(data.length>0){
		// System.out.println(Joiner.on(",").useForNull("").join(data));
		// }
		//
		//
		// }

	}

}
