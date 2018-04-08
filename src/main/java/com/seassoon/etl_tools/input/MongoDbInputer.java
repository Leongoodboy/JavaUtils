package com.seassoon.etl_tools.input;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.seassoon.utils.MongoDBUtils;

public class MongoDbInputer extends AbstractInputer {

	/**
	 * 数据库
	 */
	private String dbName;

	private String collectionName;

	/**
	 * ip
	 */
	private String IP = null;

	/**
	 * 端口
	 */
	private Integer PORT = null;

	private static Logger log = Logger.getLogger(MongoDbInputer.class);

	private MongoDBUtils mongoDBUtils;

	private MongoCursor<Document> mongoCursor;

	public MongoDbInputer(String dbName, String collectionName, String IP, Integer PORT)  {

		this.dbName = dbName;
		this.collectionName = collectionName;

		this.IP = IP;
		this.PORT = PORT;

	

	}

	@Override
	public void initialize() throws Exception {

		//mongoDBUtils = MongoDBUtils.getInstance(IP, PORT);
		mongoDBUtils=	new MongoDBUtils(IP, PORT);
		mongoCursor = mongoDBUtils.getCollection(dbName, collectionName).find().projection(Filters.eq("_id", 0))
				.iterator();

		super.initialize();

	}

	@Override
	public String[] getHeader() throws Exception {
		
		if(mongoDBUtils ==null){
			this.initialize();
		}

		if (header == null) {

			Document document = (Document) mongoDBUtils.getCollection(dbName, collectionName).find().projection(Filters.eq("_id", 0)).first();
			header = new String[document.size()];

			int count = document.size();
			for (String string : document.keySet()) {
				header[document.size()-count] = string;
				count--;
			}

		}

		return header;
	}

	@Override
	public String[] next() throws Exception {
		if (read()) {
			recordNumer++;
			return columns.toArray(new String[columns.size()]);
		}

		return null;
	}

	@Override
	public void close() throws Exception {
		//mongoDBUtils.close();

	}

	@Override
	public boolean read() {
		columns.clear();
		if (mongoCursor.hasNext()) {

			Document document = mongoCursor.next();

			List<String> headerList = Arrays.asList(header);
//			System.out.println("住院号:"+document.get("住院号"));
//			
//			if(document.get("住院号") ==null || document.getString("住院号").toString().equals("7390")){
//				System.out.println();
//			}
			//String[] tempData = new String[header.length];
			
			for (Map.Entry<String, Object> map : document.entrySet()) {

				int columnIndex = headerList.indexOf(map.getKey());

				if (columnIndex >= 0) {

					if (document.get(map.getKey()) != null) {
						//tempData[columnIndex] =document.get(map.getKey()).toString();
						
//						columns.set(columnIndex, document.get(map.getKey()).toString());
						columns.add(document.get(map.getKey()).toString());
					}else{
						columns.add(null);
					}

				}

			}

			if (!columns.isEmpty()) {
				rowNumber++;
				return true;
			}
		}

		return false;
	}

}
