package com.seassoon.etl.database.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.seassoon.etl.database.Field;
import com.seassoon.etl.database.Table;

public class OracleDatabase extends BaseDatabase {

	protected static String driver = "oracle.jdbc.driver.OracleDriver";

	@Override
	public String getUrl() {
		if (url == null) {
			url = "jdbc:oracle:thin:@" + hostname + ":" + port + ":"+databaseName;
			// url=
			// "jdbc:oracle://"+hostname+":"+port+"/"+databaseName+"?useSSL=false&zeroDateTimeBehavior=convertToNull";
		}
		return url;
	}

	@Override
	public Connection getConnection() {

		url = getUrl();

		return super.getConnection();

	}

	public OracleDatabase(String hostname, String databaseName, Integer port, String username, String password) {

		super(hostname, databaseName, port, username, password, driver);

	}

	public OracleDatabase(String url, String username, String password) {
		super(url, username, password, driver);
		// TODO Auto-generated constructor stub
	}

//	@Override
//	public List<String> getTables() {
//
//		List<Table> list = this.getTablesInfo();
//
//		List<String> tables = new ArrayList<>();
//
//		for (Table tb : list) {
//			tables.add(tb.getTbName());
//		}
//		return tables;
//	}

	@Override
	public List<String> getTables() {

		return this.getTables(null);
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
	public List<Table> getTablesInfo() {

		return this.getTablesInfo(null);

	}

//	@Override
//	public List<String> getTables(String tableName) {
//		List<Object[]> list =null;
//		if(tableName !=null && !tableName.equals("")){
//			list	 =	this.excuteQueryArray("    select * from user_tab_comments where table_type='TABLE' and table_name like '%"+tableName+"%' ", null);
//		}else{
//			list	 =	this.excuteQueryArray("   select * from user_tab_comments where table_type='TABLE'  ", null);
//		}		
//		List<String>  tables= new ArrayList<>();
//		for (Object[] object : list) {
//			tables.add(object[0].toString());
//		}
//		
//		return tables;
//		
//	}
	

	public List<Table> getTablesInfo(String tableName) {
		List<Table> tableNames = new ArrayList<Table>();
		Connection conn = getConnection();
		try {
			
			metaData = conn.getMetaData();

			if (tableName != null && !tableName.equals("")) {
				resultSet = metaData.getTables(databaseName.toUpperCase(), username.trim().toUpperCase(), tableName, new String[] { "TABLE" });
			} else {
				resultSet = metaData.getTables(null, username.trim().toUpperCase(), null, new String[] { "TABLE" }); 
			}
			while(resultSet.next()) {
				
				Table table = new Table();
				table.setRemarks(resultSet.getString("REMARKS"));
				table.setTbName(resultSet.getString("TABLE_NAME"));
//				System.out.println("tname="+table.getTbName()+",remark="+table.getRemarks());
				tableNames.add(table);
				
				
			}
			
//			List<Object[]> list =null;
//			if(tableName !=null && !tableName.equals("")){
//				list	 =	this.excuteQueryArray("    select TABLE_NAME,COMMENTS from user_tab_comments where table_type='TABLE' and table_name like '%"+tableName+"%' ", null);
//			}else{
//				list	 =	this.excuteQueryArray("   select TABLE_NAME,COMMENTS from user_tab_comments where table_type='TABLE'  ", null);
//			}
//			
//			for (Object[] object : list) {
//				Table table = new Table();
//				table.setTbName(object[0].toString());	
//				if(object[1] != null) {
//					table.setRemarks(object[1].toString());
//				}else
//					table.setRemarks(null);
//				
//				
//				tableNames.add(table);
//			}
			

		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tableNames;

		

	}

	@Override
	public List<Field> getFields(String tableName) {
		
		
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
				//field.setAutoinc(resultSet.getString("IS_AUTOINCREMENT").equals("YES"));
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
			for (Field field : fields) {
				System.out.println(field.toString());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return fields;
		
//		List<Map<String, Object>> list = null;
//		
//		if(StringUtils.isNotEmpty(tableName)){
//			 list= this.excuteQuery("SELECT d.TABLE_NAME tbName, COALESCE(t.COMMENTS, ' ') tbDesc, a.COLUMN_NAME columnName, a.DATA_TYPE columnType, a.DATA_LENGTH width, a.DATA_SCALE precision, decode(a.NULLABLE,'Y','0','1') notNull, COALESCE(m.COMMENTS, ' ') comments, decode(k.uniqueness,'UNIQUE','1','0') uniques, COALESCE(k.index_name, ' ') indexName, decode(k.key,'Y','1','0') masterKey FROM user_tab_columns a INNER JOIN user_tables d ON a.TABLE_NAME=d.TABLE_NAME LEFT JOIN user_tab_comments t ON t.TABLE_NAME=d.TABLE_NAME LEFT JOIN user_col_comments m ON m.COLUMN_NAME=a.COLUMN_NAME AND m.TABLE_NAME=d.TABLE_NAME LEFT JOIN(SELECT e.index_name, u.TABLE_NAME, u.COLUMN_NAME, e.uniqueness, decode(p.constraint_name, NULL, 'N','Y') key FROM user_indexes e INNER JOIN user_ind_columns u ON e.index_name=u.index_name LEFT JOIN (SELECT constraint_name FROM user_constraints WHERE constraint_type='P') p ON e.index_name=p.constraint_name ) k ON k.TABLE_NAME=a.TABLE_NAME AND k.COLUMN_NAME=a.COLUMN_NAME  WHERE d.TABLE_NAME ='"+tableName+"' ORDER BY tbName ");
//		}else{
//			 list= this.excuteQuery("SELECT d.TABLE_NAME tbName, COALESCE(t.COMMENTS, ' ') tbDesc, a.COLUMN_NAME columnName, a.DATA_TYPE columnType, a.DATA_LENGTH width, a.DATA_SCALE precision, decode(a.NULLABLE,'Y','0','1') notNull, COALESCE(m.COMMENTS, ' ') comments, decode(k.uniqueness,'UNIQUE','1','0') uniques, COALESCE(k.index_name, ' ') indexName, decode(k.key,'Y','1','0') masterKey FROM user_tab_columns a INNER JOIN user_tables d ON a.TABLE_NAME=d.TABLE_NAME LEFT JOIN user_tab_comments t ON t.TABLE_NAME=d.TABLE_NAME LEFT JOIN user_col_comments m ON m.COLUMN_NAME=a.COLUMN_NAME AND m.TABLE_NAME=d.TABLE_NAME LEFT JOIN(SELECT e.index_name, u.TABLE_NAME, u.COLUMN_NAME, e.uniqueness, decode(p.constraint_name, NULL, 'N','Y') key FROM user_indexes e INNER JOIN user_ind_columns u ON e.index_name=u.index_name LEFT JOIN (SELECT constraint_name FROM user_constraints WHERE constraint_type='P') p ON e.index_name=p.constraint_name ) k ON k.TABLE_NAME=a.TABLE_NAME AND k.COLUMN_NAME=a.COLUMN_NAME   ORDER BY tbName ");
//		}
//		
//		//List<Map<String, Object>> list= this.excuteQuery("SELECT d.TABLE_NAME tbName, COALESCE(t.COMMENTS, ' ') tbDesc, a.COLUMN_NAME columnName, a.DATA_TYPE columnType, a.DATA_LENGTH width, a.DATA_SCALE precision, decode(a.NULLABLE,'Y','0','1') notNull, COALESCE(m.COMMENTS, ' ') comments, decode(k.uniqueness,'UNIQUE','1','0') uniques, COALESCE(k.index_name, ' ') indexName, decode(k.key,'Y','1','0') masterKey FROM user_tab_columns a INNER JOIN user_tables d ON a.TABLE_NAME=d.TABLE_NAME LEFT JOIN user_tab_comments t ON t.TABLE_NAME=d.TABLE_NAME LEFT JOIN user_col_comments m ON m.COLUMN_NAME=a.COLUMN_NAME AND m.TABLE_NAME=d.TABLE_NAME LEFT JOIN(SELECT e.index_name, u.TABLE_NAME, u.COLUMN_NAME, e.uniqueness, decode(p.constraint_name, NULL, 'N','Y') key FROM user_indexes e INNER JOIN user_ind_columns u ON e.index_name=u.index_name LEFT JOIN (SELECT constraint_name FROM user_constraints WHERE constraint_type='P') p ON e.index_name=p.constraint_name ) k ON k.TABLE_NAME=a.TABLE_NAME AND k.COLUMN_NAME=a.COLUMN_NAME  WHERE d.TABLE_NAME ='"+tableName+"' ORDER BY tbName ");
//		
//		
//		List<Field> fields= new ArrayList<>();
//		
//		
//		for (Map<String,Object> map : list) {
//			Field field=new Field();
//			field.setTableName(map.get("TBNAME").toString());
//			field.setName(map.get("COLUMNNAME").toString());
//			field.setType(map.get("COLUMNTYPE").toString());			
//			//field.setAutoinc(map.get("标识").toString().equals("√"));
//			field.setAllowNull(map.get("NOTNULL").toString().equals("0")); 
//			field.setPrimarKey(map.get("MASTERKEY").toString().equals("1"));
//			fields.add(field);
//		}
		
		
		
//		return fields;
	} 
	
	@Override
	public boolean existsTable(String tableName) {
		List<Table> list = this.getTablesInfo(tableName);
		if (list.size() >= 0) {
			return true;
		}

		return false;
	}

	@Override
	public ResultSet query(String sql, Object... params) {
		// TODO Auto-generated method stub
		return executeQueryRS(sql, params);
	}

	public static void main(String[] args) {

		OracleDatabase database = new OracleDatabase("10.50.5.2", "ORCL", 1521, "CITYGRID", "sxad1357924680");

		List<Field> fields = database.getFields("T_INFO_MAIN");

		for (Field field : fields) {
			System.out.println(field.getName() + "——" + field.getRemarks());
		}
		System.out.println("------------");
		List<Table> tables = database.getTablesInfo();
		for (Table tableName : tables) {
			System.out.println(tableName.getTbName() + ",注释：" + tableName.getRemarks());
		}
	}

}
