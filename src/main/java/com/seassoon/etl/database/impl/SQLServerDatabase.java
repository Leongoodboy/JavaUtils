package com.seassoon.etl.database.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.seassoon.etl.database.Field;
import com.seassoon.etl.database.Table;

public class SQLServerDatabase extends BaseDatabase {
	
	
	protected static String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	

			
 
	@Override
	public String getUrl() {
		if(url==null){
//			url= "jdbc:mysql://"+hostname+":"+port+"/"+databaseName+"?useSSL=false&zeroDateTimeBehavior=convertToNull";
			
			url= "jdbc:sqlserver://"+hostname+":"+port+";DatabaseName="+databaseName;
			System.out.println(url);
		}
		return url;
	}
	
	
	@Override
	public Connection getConnection() {
		
		
		url =getUrl();

		return super.getConnection();
	}

	public SQLServerDatabase(String hostname, String databaseName, Integer port, String username, String password) {
		
		super(hostname, databaseName, port, username, password,driver);

	}
	



	public SQLServerDatabase(String url, String username, String password) {
		super(url, username, password, driver);
		// TODO Auto-generated constructor stub
	}

//	@Override
//	public List<String> getTables(String tableName) {
//		List<Object[]> list =null;
//		if(tableName !=null && !tableName.equals("")){
//			list	 =	this.excuteQueryArray(" SELECT Name FROM SysObjects Where XType='U'  and Name like '%"+tableName+"%' ", null);
//		}else{
//			list	 =	this.excuteQueryArray(" SELECT Name FROM SysObjects Where XType='U'  ", null);
//		}
//		
//	
//		
//		
//		List<String>  tables= new ArrayList<>();
//		
//		for (Object[] object : list) {
//			tables.add(object[0].toString());
//		}
//		
//		return tables;
//		
//	}
	
	
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
	public List<String> getTables() {
		
//		
//		List<Object[]> list =	this.excuteQueryArray("show tables ", null);
//		
//		List<String>  tables= new ArrayList<>();
//		
//		for (Object[] object : list) {
//			tables.add(object[0].toString());
//		}
		
		return this.getTables(null);
		
	}
	
		


	/*@Override
	public List<Field> getFields(String tableName) {
		
		List<Map<String, Object>> list = null;
		
		if(StringUtils.isNotEmpty(tableName)){
			 list= this.excuteQuery("SELECT(case WHEN a.colorder=1 THEN d.name ELSE NULL end) 表名,a.name 字段名, (case WHEN COLUMNPROPERTY( a.id,a.name,'IsIdentity')=1 THEN '√'else '' end) 标识, (case WHEN (SELECT count(*) FROM sysobjects WHERE (name IN (SELECT name FROM sysindexes WHERE (id = a.id) AND (indid IN (SELECT indid FROM sysindexkeys WHERE (id = a.id) AND (colid IN (SELECT colid FROM syscolumns WHERE (id = a.id) AND (name = a.name))))))) AND (xtype = 'PK'))>0 THEN '√' ELSE '' end) 主键,b.name 类型,a.length 占用字节数, COLUMNPROPERTY(a.id,a.name,'PRECISION') AS 长度, isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0) AS 小数位数,(case WHEN a.isnullable=1 THEN '√'else '' end) 允许空, isnull(e.text,'') 默认值 FROM syscolumns a LEFT JOIN systypes b ON a.xtype=b.xusertype INNER JOIN sysobjects d ON a.id=d.id AND d.xtype='U' AND d.name<>'dtproperties' LEFT JOIN syscomments e ON a.cdefault=e.id LEFT JOIN sys.extended_properties g ON a.id=g.major_id AND a.colid=g.minor_id LEFT JOIN sys.extended_properties f ON d.id=f.class AND f.minor_id=0 WHERE b.name is NOT null AND d.name='"+tableName+"'  ORDER BY a.id,a.colorder");
		}else{
			 list= this.excuteQuery("SELECT(case WHEN a.colorder=1 THEN d.name ELSE NULL end) 表名,a.name 字段名, (case WHEN COLUMNPROPERTY( a.id,a.name,'IsIdentity')=1 THEN '√'else '' end) 标识, (case WHEN (SELECT count(*) FROM sysobjects WHERE (name IN (SELECT name FROM sysindexes WHERE (id = a.id) AND (indid IN (SELECT indid FROM sysindexkeys WHERE (id = a.id) AND (colid IN (SELECT colid FROM syscolumns WHERE (id = a.id) AND (name = a.name))))))) AND (xtype = 'PK'))>0 THEN '√' ELSE '' end) 主键,b.name 类型,a.length 占用字节数, COLUMNPROPERTY(a.id,a.name,'PRECISION') AS 长度, isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0) AS 小数位数,(case WHEN a.isnullable=1 THEN '√'else '' end) 允许空, isnull(e.text,'') 默认值 FROM syscolumns a LEFT JOIN systypes b ON a.xtype=b.xusertype INNER JOIN sysobjects d ON a.id=d.id AND d.xtype='U' AND d.name<>'dtproperties' LEFT JOIN syscomments e ON a.cdefault=e.id LEFT JOIN sys.extended_properties g ON a.id=g.major_id AND a.colid=g.minor_id LEFT JOIN sys.extended_properties f ON d.id=f.class AND f.minor_id=0 WHERE b.name is NOT null  ORDER BY a.id,a.colorder");
		}
		

		
		
		List<Field> fields= new ArrayList<>();
		
		
		String currentTableName=null;
		
		for (Map<String,Object> map : list) {
			if(map.get("表名") !=null){
				currentTableName = map.get("表名").toString();
			}else{
				map.put("表名", currentTableName);
			}
			
		}
		 
		for (Map<String,Object> map : list) {
			Field field=new Field();
			field.setTableName(map.get("表名").toString());
			field.setName(map.get("字段名").toString());
			field.setType(map.get("类型").toString());			
			field.setAutoinc(map.get("标识").toString().equals("√"));
			field.setAllowNull(map.get("允许空").toString().equals("√")); 
			field.setPrimarKey(map.get("主键").toString().equals("√"));
			fields.add(field);
		}
		
		
		
		return fields;
	} */
	
	
	
	@Override
	public List<Field> getFields(String tableName) {
		
		
		List<Field> fields = new ArrayList<>();
		List<Field> fieldsWithRemark = new ArrayList<>();

		Connection conn = getConnection();
		try {
			
			metaData = conn.getMetaData();
			resultSet = metaData.getColumns(databaseName, null, tableName, "%");

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
//			for (Field field : fields) {
//				System.out.println(field.toString());
//			}
			
			//查询注释
			StringBuffer sb= new StringBuffer();
			sb.append("SELECT")
			.append(" B.name AS column_name,")
			.append(" cast(C.value  as varchar(500)) AS column_description")
			.append(" FROM sys.tables A")
			.append(" INNER JOIN sys.columns B ON B.object_id = A.object_id")
			.append(" LEFT JOIN sys.extended_properties C ON C.major_id = B.object_id AND C.minor_id = B.column_id")
			.append(" WHERE A.name = '").append(tableName).append("'");
			String sql=sb.toString();
			List<Map<String, Object>> list= this.excuteQuery(sql);
			HashMap<String, String> descMap=new HashMap<>();
			for (Map<String,Object> map : list) {				
				descMap.put(map.get("column_name").toString(),map.get("column_description").toString());
			}
			
			
			for (Field field : fields) {
				field.setRemarks(descMap.get(field.getName()));
				fieldsWithRemark.add(field);
			}
			for (Field field : fieldsWithRemark) {
				System.out.println(field.toString());
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return fieldsWithRemark;

	} 
	
	@Override
	public boolean existsTable(String tableName) {
		List<String> list= this.getTables(tableName);
		if(list.size()>=0){
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
		
		SQLServerDatabase database=new SQLServerDatabase("192.168.2.193", "syncTest", 1433, "sa", "x123456");
		System.out.println(database.getTables());
		
//		
		List<Field> fields =database.getFields("test");
		
		for (Field field : fields) {
			System.out.println(field.getName()+","+field.getRemarks());
		} 
		System.out.println("-------------------");
		
		List<String> tables =database.getTables();
		for (String tableName : tables) {
			System.out.println(tableName); 
		}
		
		 List<Table> list=database.getTablesInfo();
		 for (Table tb : list) {
				System.out.println(tb.getTbName()+","+tb.getRemarks());
				
			}
	}


	@Override
	public List<Table> getTablesInfo() {
		// TODO Auto-generated method stub
		return this.getTablesInfo(null);
	}


	/*
	public List<Table> getTablesInfo_bak(String tableName) {
		List<Table> tableNames = new ArrayList<Table>();
		Connection conn = getConnection();
		try {
			
			metaData = conn.getMetaData();

	
			
			List<Object[]> list =null;
			if(tableName !=null && !tableName.equals("")){
				list	 =	this.excuteQueryArray("    select   sysobjects.name,cast(sys.extended_properties.value  as varchar(500)) value    from   sysobjects left join sys.extended_properties on sysobjects.id=sys.extended_properties.major_id  where   type= 'U '  and table_name like '%"+tableName+"%' "+"order by name ", null);
			}else{
				list	 =	this.excuteQueryArray("   select   sysobjects.name,cast(sys.extended_properties.value  as varchar(500)) value   from   sysobjects left join sys.extended_properties on sysobjects.id=sys.extended_properties.major_id  where   type= 'U '  order by name  ", null);
			}
			
			for (Object[] object : list) {
				Table table = new Table();
				table.setTbName(object[0].toString());	
				if(object[1] != null) {
					table.setRemarks(object[1].toString());
				}else
					table.setRemarks(null);
				
				
				tableNames.add(table);
			}
			

		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tableNames;

	}

	*/
	
	@Override
	public List<Table> getTablesInfo(String tableName) {
		List<Table> tableNames = new ArrayList<Table>();
		Connection conn = getConnection();
		try {
			
			metaData = conn.getMetaData();

			if (tableName != null && !tableName.equals("")) {
				resultSet = metaData.getTables(databaseName, username, tableName, new String[] { "TABLE" });
			} else {
				resultSet = metaData.getTables(null, null, null, new String[] { "TABLE" }); 
			}
			
			while(resultSet.next()) {
				
				Table table = new Table();
				table.setRemarks(resultSet.getString("REMARKS"));
				table.setTbName(resultSet.getString("TABLE_NAME"));
				tableNames.add(table);
				
				
			}
			
			
//			List<Object[]> list =null;
//			if(tableName !=null && !tableName.equals("")){
//				list	 =	this.excuteQueryArray("    select   sysobjects.name,cast(sys.extended_properties.value  as varchar(500)) value    from   sysobjects left join sys.extended_properties on sysobjects.id=sys.extended_properties.major_id  where   type= 'U ' and sys.extended_properties.minor_id='0'  and table_name like '%"+tableName+"%' "+"order by name ", null);
//			}else{
//				list	 =	this.excuteQueryArray("   select   sysobjects.name,cast(sys.extended_properties.value  as varchar(500)) value   from   sysobjects left join sys.extended_properties on sysobjects.id=sys.extended_properties.major_id  where   type= 'U ' and sys.extended_properties.minor_id='0' order by name  ", null);
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

	
	


}
