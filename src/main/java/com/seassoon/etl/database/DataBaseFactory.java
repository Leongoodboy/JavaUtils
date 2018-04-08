package com.seassoon.etl.database;

import com.seassoon.etl.database.impl.MysqlDatabase;
import com.seassoon.etl.database.impl.OracleDatabase;
import com.seassoon.etl.database.impl.PostgreSqlDataBase;
import com.seassoon.etl.database.impl.SQLServerDatabase;

/**
 * 数据库工厂类
 * 
 * @author zhangqianfeng
 *
 */
public class DataBaseFactory {

	public enum DataBaseTypeEnum {
		Mysql, Oracle, SQLServer, Postgresql
	}

	public static DatabaseInterface getDatabaseInterface(String type, String hostname, String databaseName,
			Integer port, String username, String password) {

		if (type == null) {
			return null;
		}
		if (type.toLowerCase().equals(DataBaseTypeEnum.Mysql.toString().toLowerCase())) {
			return new MysqlDatabase(hostname, databaseName, port, username, password);
		} else if (type.toLowerCase().equals(DataBaseTypeEnum.SQLServer.toString().toLowerCase())) {
			return new SQLServerDatabase(hostname, databaseName, port, username, password);
		} else if (type.toLowerCase().equals(DataBaseTypeEnum.Oracle.toString().toLowerCase())) {
			return new OracleDatabase(hostname, databaseName, port, username, password);
		} else if (type.toLowerCase().equals(DataBaseTypeEnum.Postgresql.toString().toLowerCase())) {
			return new PostgreSqlDataBase(hostname, databaseName, port, username, password);
		}

		return null;

	}

	public static DatabaseInterface getDatabaseInterface(String type, String url, String username, String password) {

		if (type == null) {
			return null;
		} 
		System.out.println("db_type:=========================="+type);
		if (type.toLowerCase().equals(DataBaseTypeEnum.Mysql.toString().toLowerCase())) {
			return new MysqlDatabase(url, username, password);
		} else if (type.toLowerCase().equals(DataBaseTypeEnum.SQLServer.toString().toLowerCase())) {
			return new SQLServerDatabase(url, username, password);
		} else if (type.toLowerCase().equals(DataBaseTypeEnum.Oracle.toString().toLowerCase())) {
			return new OracleDatabase(url, username, password);
		} else if (type.toLowerCase().equals(DataBaseTypeEnum.Postgresql.toString().toLowerCase())) {
			return new PostgreSqlDataBase(url, username, password);
		}
 
		return null;

	}

}
