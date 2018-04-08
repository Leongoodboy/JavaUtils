package com.seassoon.etl.database.impl;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.seassoon.etl.database.Field;
import com.seassoon.etl.database.Table;

public class MysqlDatabase extends BaseDatabase {
	
	
	protected static String driver = "com.mysql.jdbc.Driver";
	
	@Override
	public String getUrl() {
		if(url==null){
			url= "jdbc:mysql://"+hostname+":"+port+"/"+databaseName+"?characterEncoding=utf8&useSSL=false&zeroDateTimeBehavior=convertToNull";
		}
		return url;
	}

	@Override
	public Connection getConnection() {
		
		url =getUrl();
	

		return super.getConnection();
	}

	public MysqlDatabase(String hostname, String databaseName, Integer port, String username, String password) {
		
		super(hostname, databaseName, port, username, password,driver);

	}
	



	public MysqlDatabase(String url, String username, String password) {
		super(url, username, password, driver);
		// TODO Auto-generated constructor stub
	}
	@Override
	public List<String> getTables() {

		List<Table> list = this.getTablesInfo();

		List<String> tables = new ArrayList<>();

		for (Table tb : list) {
			tables.add(tb.getTbName());
		}
		return tables;
	}
	@Override
	public List<String> getTables(String tableName) {
		List<Table> list = this.getTablesInfo(tableName);

		List<String> tables = new ArrayList<>();

		for (Table tb : list) {
			tables.add(tb.getTbName());
		}
		return tables;

	}
	@Override
	public List<Table> getTablesInfo(String tableName) {
		List<Table> tableNames = new ArrayList<Table>();
		Connection conn = getConnection();
		try {
			metaData = conn.getMetaData();

			if (tableName != null && !tableName.equals("")) {
				resultSet = metaData.getTables(databaseName, "%", tableName, new String[] { "TABLE" });
			} else {
				resultSet = metaData.getTables(databaseName, "%", "%", new String[] { "TABLE" });
			}

			while (resultSet.next()) {
				Table table = new Table();
				table.setRemarks(resultSet.getString("REMARKS"));
				table.setTbName(resultSet.getString("TABLE_NAME"));
				tableNames.add(table);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tableNames;
		
	}
	
	@Override
	public List<Table> getTablesInfo() {
		
		return this.getTablesInfo(null);
		
	}

	@Override
	public List<Field> getFields(String tableName) {
		
//		List<Map<String, Object>> list= this.excuteQuery("desc "+tableName);
		List<Field> fields = new ArrayList<>();

		Connection conn = getConnection();
		try {
			metaData = conn.getMetaData();
			resultSet = metaData.getColumns(databaseName, "%", tableName, "%");

			while (resultSet.next()) {
				Field field = new Field();
				field.setTableName(resultSet.getString("TABLE_NAME").toString());
				field.setName(resultSet.getString("COLUMN_NAME"));
				field.setType(resultSet.getString("TYPE_NAME"));
				field.setAutoinc(resultSet.getString("IS_AUTOINCREMENT").equals("YES"));
				field.setAllowNull(resultSet.getString("IS_NULLABLE").equals("YES"));
				field.setPrimarKey(resultSet.getString("TYPE_NAME").equals("YES"));
				field.setRemarks(resultSet.getString("REMARKS"));
				fields.add(field);
			}
			ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(databaseName, username, tableName);
			while (primaryKeyResultSet.next()) {
				String colName = primaryKeyResultSet.getString("COLUMN_NAME");
				for (Field field : fields) {
					if (field.getName().equals(colName)) {
						field.setPrimarKey(true);
						break;
					}
				}
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		}

		return fields;
	} 
	
	
	@Override
	public boolean existsTable(String tableName) {
		
		List<Table> list= this.getTablesInfo(tableName);
		if(list.size()>=0){
			return true;
		}
		
		return false;
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		
		MysqlDatabase database=new MysqlDatabase("localhost", "db_qcc", 3306, "root", "123");
		
		
		List<Field> fields =database.getFields("qcc_beizhixingren");
		
		for (Field field : fields) {
			System.out.println(field);
		}
		
		List<Table> tables =database.getTablesInfo();
		for (Table tableName : tables) {
			System.out.println(tableName.getTbName()+",表格注释："+tableName.getRemarks());
		}
	}

	@Override
	public ResultSet query(String sql, Object... params) {
		// TODO Auto-generated method stub
		return executeQueryRS(sql, params);
	}
}
