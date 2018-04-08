package com.seassoon.etl_tools.outout;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.seassoon.etl_tools.outout.OutPuter;
import com.seassoon.utils.MongoDBUtils;

public class MongoDBOutputer extends AbstractOutPuter {

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

	private static Logger log = Logger.getLogger(MongoDBOutputer.class);

	private MongoDBUtils mongoDBUtils;

	private Integer submitSize = 20000;

	private List<String[]> list = new ArrayList<>();

	public Integer getSubmitSize() {
		return submitSize;
	}

	public void setSubmitSize(Integer submitSize) {
		this.submitSize = submitSize;
	}

	public MongoDBOutputer(String dbName, String collectionName, String IP, Integer PORT,String[] header) {

		this.dbName = dbName;
		this.collectionName = collectionName;

		this.IP = IP;
		this.PORT = PORT;

		this.header = header;

	}

	public void initialize() {
		if (mongoDBUtils == null) {
		//	mongoDBUtils = MongoDBUtils.getInstance(IP, PORT);
			mongoDBUtils=	new MongoDBUtils(IP, PORT);
		}

	}

	@Override
	public void process(String[] data) {
		
		if (mongoDBUtils == null) {
			this.initialize();
		}
		

		if (data == null) {
			execute();
		} else {
			processNum.incrementAndGet();

			list.add(data);
			if (list.size() > submitSize) {
				execute();
			}

		}

	}

	public void execute() {
		List<Object> paramsList = new ArrayList<Object>();

		Document doc;

		List<Document> docs = new ArrayList<>();
		for (String[] temp : list) {
			doc = new Document();
			for (int i = 0; i < temp.length; i++) {
				doc.append(header[i].replace(".", ""), temp[i]);
			}
			docs.add(doc);
//			System.out.println(	mongoDBUtils.insert(dbName, collectionName, doc));
		}

	  	System.out.println(	mongoDBUtils.insertMany(dbName, collectionName, docs));
		// db.insert(columnsList.toArray(new String[columnsList.size()]),
		// paramsList.toArray(), tableName);
		list.clear();
	}

	@Override
	public void close() {
		this.execute();
		// try {
		//
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

	}

}
