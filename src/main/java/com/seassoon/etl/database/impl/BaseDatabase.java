package com.seassoon.etl.database.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.seassoon.etl.database.DatabaseInterface;
import com.seassoon.etl.database.Field;
import com.seassoon.etl.database.Table;

public abstract class BaseDatabase implements DatabaseInterface {

	private Logger log = Logger.getLogger(BaseDatabase.class);

	/**
	 * 数据库驱动类名称
	 */
	protected String driver = null;

	// /**
	// * 连接字符串
	// */
	// private String URL = null;

	protected String hostname;
	protected String databaseName;
	protected Integer port;
	protected String username;
	protected String password;

	/**
	 * 连接字符串
	 */
	protected String url;

	/**
	 * 创建数据库连接对象
	 */
	protected Connection connnection = null;

	/**
	 * 创建PreparedStatement对象
	 */
	protected PreparedStatement preparedStatement = null;

	/**
	 * 创建Statement对象
	 */
	protected Statement statement = null;

	/**
	 * 创建CallableStatement对象
	 */
	protected CallableStatement callableStatement = null;

	/**
	 * 创建结果集对象
	 */
	protected ResultSet resultSet = null;

	protected DatabaseMetaData metaData = null;
	// static {
	// try {
	// // 加载数据库驱动程序
	// Class.forName(DRIVER);
	// } catch (ClassNotFoundException e) {
	// System.out.println("加载驱动错误");
	// System.out.println(e.getMessage());
	// }
	// }

	public BaseDatabase(String hostname, String databaseName, Integer port, String username, String password,
			String driver) {
		super();
		this.hostname = hostname;
		this.databaseName = databaseName;
		this.port = port;
		this.username = username;
		this.password = password;
		this.driver = driver;
	}

	public BaseDatabase(String url, String username, String password, String driver) {
		super();
		this.url = url;
		this.username = username;
		this.password = password;
		this.driver = driver;
	}

	public String getUrl() {

		return url;
	}

	/**
	 * 建立数据库连接
	 * 
	 * @return 数据库连接
	 */
	public Connection getConnection() {

		try {
			if (connnection == null || connnection.isClosed() == true) {

				Class.forName(driver);
				// 获取连接
				Properties props = new Properties();
				props.put("remarksReporting", "true");
				props.setProperty("remarks", "true");
				props.put("user", username);
				props.put("password", password);
				props.setProperty("useInformationSchema", "true");// 设置可以获取tables
																	// remarks信息
				props.setProperty("charset", "utf-8");
				connnection = DriverManager.getConnection(url, props);

			}

		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connnection;
	}

	/**
	 * insert update delete SQL语句的执行的统一方法
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 受影响的行数
	 */
	public int executeUpdate(String sql, Object[] params) {
		// 受影响的行数
		int affectedLine = 0;

		try {
			// 获得连接
			connnection = this.getConnection();
			// 调用SQL
			preparedStatement = connnection.prepareStatement(sql);

			// 参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					preparedStatement.setObject(i + 1, params[i]);
				}
			}

			// 执行
			affectedLine = preparedStatement.executeUpdate();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			// 释放资源
			// closeAll();
		}
		return affectedLine;
	}

	/**
	 * insert update delete SQL语句的执行的统一方法
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 受影响的行数
	 */
	public int executeUpdate(String sql) {
		// 受影响的行数
		int affectedLine = 0;

		try {
			// 获得连接
			connnection = this.getConnection();
			// 调用SQL
			statement = connnection.createStatement();

			// 执行
			affectedLine = statement.executeUpdate(sql);

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			// 释放资源
			// closeAll();
		}
		return affectedLine;
	}

	/**
	 * SQL 查询将查询结果直接放入ResultSet中
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 结果集
	 */
	public ResultSet executeQueryRS(String sql, Object[] params) {
		try {

			// 获得连接
			connnection = this.getConnection();

			// 调用SQL
			preparedStatement = connnection.prepareStatement(sql);

			if (this.driver.contains("mysql")) {
				preparedStatement.setFetchSize(Integer.MIN_VALUE);
			} else {
				preparedStatement.setFetchSize(100);
			}

			// 参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					preparedStatement.setObject(i + 1, params[i]);
				}
			}

			// 执行
			resultSet = preparedStatement.executeQuery();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		return resultSet;
	}

	/**
	 * SQL 查询将查询结果直接放入ResultSet中
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 结果集
	 */
	public ResultSet executeQueryRSBigData(String sql, Object[] params) {
		try {

			// 获得连接
			connnection = this.getConnection();

			// 调用SQL
			// preparedStatement = connnection.prepareStatement(sql);
			preparedStatement = (PreparedStatement) connnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);

			preparedStatement.setFetchSize(Integer.MIN_VALUE);

			preparedStatement.setFetchDirection(ResultSet.FETCH_REVERSE);

			// 参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					preparedStatement.setObject(i + 1, params[i]);
				}
			}

			// 执行
			resultSet = preparedStatement.executeQuery();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		return resultSet;
	}

	/**
	 * SQL 查询将查询结果：一行一列
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 结果集
	 */
	public Object executeQuerySingle(String sql, Object[] params) {
		Object object = null;
		try {
			// 获得连接
			connnection = this.getConnection();

			// 调用SQL
			preparedStatement = connnection.prepareStatement(sql);

			// 参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					preparedStatement.setObject(i + 1, params[i]);
				}
			}

			// 执行
			resultSet = preparedStatement.executeQuery();

			if (resultSet.next()) {
				object = resultSet.getObject(1);
			}

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			closeAll();
		}

		return object;
	}

	/**
	 * 获取结果集，并将结果放在List中
	 * 
	 * @param sql
	 *            SQL语句
	 * @return List 结果集
	 */
	public List<Map<String, Object>> excuteQuery(String sql, Object... params) {

		// 执行SQL获得结果集
		ResultSet rs = executeQueryRS(sql, params);

		// 创建ResultSetMetaData对象
		ResultSetMetaData rsmd = null;

		// 结果集列数
		int columnCount = 0;
		try {
			rsmd = rs.getMetaData();

			// 获得结果集列数
			columnCount = rsmd.getColumnCount();
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
		} catch (NullPointerException e1) {
			e1.printStackTrace();
			System.out.println(e1.getMessage());
		}

		// 创建List
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		try {
			// 将ResultSet的结果保存到List中
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 1; i <= columnCount; i++) {
					map.put(rsmd.getColumnLabel(i), rs.getObject(i));
				}
				list.add(map);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			// 关闭所有资源
			closeAll();
			// try {
			//
			// rs.close();
			// } catch (SQLException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		}

		return list;
	}

	/**
	 * 获取结果集，并将结果放在List中
	 * 
	 * @param sql
	 *            SQL语句
	 * @return List 结果集
	 */
	public List<Object[]> excuteQueryArray(String sql, Object[] params) {

		// 执行SQL获得结果集
		ResultSet rs = executeQueryRS(sql, params);

		// 创建ResultSetMetaData对象
		ResultSetMetaData rsmd = null;

		// 结果集列数
		int columnCount = 0;
		try {
			rsmd = rs.getMetaData();

			// 获得结果集列数
			columnCount = rsmd.getColumnCount();
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
		}

		// 创建List
		List<Object[]> list = new ArrayList<Object[]>();

		try {
			// 将ResultSet的结果保存到List中
			while (rs.next()) {

				Object[] array = new Object[columnCount];
				for (int i = 1; i <= columnCount; i++) {

					array[i - 1] = rs.getObject(i);
				}
				list.add(array);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			// 关闭所有资源
			closeAll();
			// try {
			//
			// rs.close();
			// } catch (SQLException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		}

		return list;
	}

	public void insert(String[] colums, Object[] params, String tableName) {

		StringBuffer sql = new StringBuffer();
		// ignore
		sql.append(" insert  ignore   into `" + tableName + "`  ( `" + Joiner.on("`,`").useForNull("").join(colums)
				+ "`  )   values  ");

		List<String> recordList = new ArrayList<>();
		List<String> paramList = new ArrayList<>();

		for (int i = 1; i <= params.length; i++) {

			paramList.add("?");

			if (i >= colums.length && i % colums.length == 0) {
				recordList.add("(" + Joiner.on(",").join(paramList) + ")");
				paramList.clear();
			}

		}

		sql.append(Joiner.on(",").join(recordList));

		// System.out.println(sql .toString());

		int result = this.executeUpdate(sql.toString(), params);

		log.info(tableName + "新增:" + result);

		this.closeAll();

	}

	/**
	 * 存储过程带有一个输出参数的方法
	 * 
	 * @param sql
	 *            存储过程语句
	 * @param params
	 *            参数数组
	 * @param outParamPos
	 *            输出参数位置
	 * @param SqlType
	 *            输出参数类型
	 * @return 输出参数的值
	 */
	public Object excuteQuery(String sql, Object[] params, int outParamPos, int SqlType) {
		Object object = null;
		connnection = this.getConnection();
		try {
			// 调用存储过程
			callableStatement = connnection.prepareCall(sql);

			// 给参数赋值
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					callableStatement.setObject(i + 1, params[i]);
				}
			}

			// 注册输出参数
			callableStatement.registerOutParameter(outParamPos, SqlType);

			// 执行
			callableStatement.execute();

			// 得到输出参数
			object = callableStatement.getObject(outParamPos);

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			// 释放资源
			closeAll();
		}

		return object;
	}

	/**
	 * 关闭所有资源
	 */
	public void closeAll() {
		// 关闭结果集对象
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}

		// 关闭PreparedStatement对象
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		// 关闭PreparedStatement对象
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}

		// 关闭CallableStatement 对象
		if (callableStatement != null) {
			try {
				callableStatement.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}

		// 关闭Connection 对象
		if (connnection != null) {
			try {
				connnection.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
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

		// List<Map<String, Object>> list= this.excuteQuery("desc "+tableName);
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

		List<Table> list = this.getTablesInfo(tableName);
		if (list.size() >= 0) {
			return true;
		}

		return false;
	}

}
