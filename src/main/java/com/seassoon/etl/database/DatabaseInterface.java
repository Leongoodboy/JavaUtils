package com.seassoon.etl.database;

import java.sql.ResultSet;
import java.util.List;


/**
 * 数据库操作接口
 * @author zhangqianfeng
 *
 */
public interface DatabaseInterface {
	

	/**
	 * 是否已存在表
	 * @param tableName
	 * @return
	 */
	public boolean existsTable(String tableName);
	/**
	 * 获取所有表
	 * @return
	 */
	public List<String> getTables();
	/**
	 * 模糊匹配表
	 * @return
	 */
	public List<String> getTables(String tableName);
	
	public List<Table> getTablesInfo();
	
	public List<Table> getTablesInfo(String tableName);
	/**
	 * 获取表所有字段信息
	 * @param tableName
	 * @return
	 */
	public List<Field> getFields(String tableName);
	
	/**
	 * SQL 查询将查询结果直接放入ResultSet中
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 结果集
	 */
	public ResultSet query(String sql, Object... params);
	
	
	/**
	 * insert update delete SQL语句的执行的统一方法
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数数组，若没有参数则为null
	 * @return 受影响的行数
	 */
	public int executeUpdate(String sql, Object[] params) ;
	
	
	/**
	 * 关闭
	 */
	public void closeAll();
	
	public String getUrl();
	
}
