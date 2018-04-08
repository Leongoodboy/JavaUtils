package etl_tools;

import org.apache.log4j.chainsaw.Main;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.seassoon.utils.MongoDBUtils;

public class MongoDbTest {

	
	public static void main(String[] args) {
		
		
		MongoDBUtils mongoDBUtils= MongoDBUtils.getInstance("172.16.40.10", 30000);
		
		
		MongoCollection<Document> col= mongoDBUtils.getCollection("mydb", "users");
		
		for (int i = 0; i < 100000; i++) {
			Document document =new Document("index",i);
			col.insertOne(document);
			System.out.println(i);
		}
		
		
	}
}
