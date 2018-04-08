package com.seassoon.utils;

public enum JDBC_DRIVER {
	MYSQL("com.mysql.jdbc.Driver"), ORACLE("oracle.jdbc.driver.OracleDriver");

	private final String value;

	JDBC_DRIVER(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
