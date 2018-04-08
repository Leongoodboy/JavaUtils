package etl_tools;

import java.util.List;

import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.input.MongoDbInputer;
import com.seassoon.etl_tools.outout.MongoDBOutputer;
import com.seassoon.utils.MongoDBUtils;

public class mongo_mongo {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		MongoDBUtils mongoDBUtils = MongoDBUtils.getInstance("10.50.5.14", 28117);

		List<String> list = mongoDBUtils.getAllCollections("gzfy");

		for (String col : list) {

			MongoDbInputer inputer = new MongoDbInputer("gzfy", col, "10.50.5.14", 28117);

			inputer.initialize();

			MongoDBOutputer outputer = new MongoDBOutputer("gzfy", col, "118.190.98.2", 28117, inputer.getHeader());

			outputer.initialize(); 

			inputer.out(outputer);
		}

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
