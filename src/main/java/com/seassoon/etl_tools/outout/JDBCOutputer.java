package com.seassoon.etl_tools.outout;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.seassoon.etl_tools.outout.OutPuter;
import com.seassoon.utils.ConnectionDB;

public class JDBCOutputer extends AbstractOutPuter {

	public String sql;

	public ConnectionDB db;

	/**
	 * 数据库驱动类名称
	 */
	private String DRIVER = "com.mysql.jdbc.Driver";

	/**
	 * 连接字符串
	 */
	private String URL = null;
	/**
	 * 用户名
	 */
	private String USERNAME = null;

	/**
	 * 密码
	 */
	private String PASSWORD = null;

	private static Logger log = Logger.getLogger(JDBCOutputer.class);

	private String tableName;

	private Integer submitSize = 100000;

	private List<String[]> list = new ArrayList<>();

	public Integer getSubmitSize() {
		return submitSize;
	}

	public void setSubmitSize(Integer submitSize) {
		this.submitSize = submitSize;
	}

	public JDBCOutputer(String tableName, String[] header) {

		this.header = header;

		this.tableName = tableName;

	}
	
	public JDBCOutputer(String tableName, String[] header, String URL, String USERNAME, String PASSWORD) {

		super();
		this.tableName = tableName;
		// this.db = db;
		this.header = header;

		this.URL = URL;
		this.USERNAME = USERNAME;
		this.PASSWORD = PASSWORD;
	

	}

	public JDBCOutputer(String tableName, String[] header, String URL, String USERNAME, String PASSWORD,
			String DRIVER) {

		super();
		this.tableName = tableName;
		// this.db = db;
		this.header = header;

		this.URL = URL;
		this.USERNAME = USERNAME;
		this.PASSWORD = PASSWORD;
		this.DRIVER = DRIVER;

	}

	public void initialize() {

		db = new ConnectionDB(URL, USERNAME, PASSWORD, DRIVER);
		
	}

	@Override
	public void process(String[] data) throws Exception {
		
		if (db == null) {
			this.initialize();
		}
		
		

		if (data == null) {
			execute();
		} else {

			if(header.length  != data.length){
				throw new Exception("插入数据列不一致！");
			}
			processNum.incrementAndGet();

			list.add(data);
			if (list.size() > submitSize) {
				execute();
			}

		}

	}

	public void execute() {
		
		List<Object> paramsList = new ArrayList<Object>();
		for (String[] temp : list) {
			for (String string : temp) {
				paramsList.add(string);
			}

		}
		
		db.insert(header, paramsList.toArray(), tableName);
		list.clear();

		System.out.println(tableName + " output " + processNum);
		
		
		
	}

	@Override
	public void close() {

		this.execute();

		db.closeAll();
		// try {
		//
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
 
	}

}
