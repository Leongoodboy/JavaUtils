package com.seassoon.etl.database;

/**
 * 字段属性
 * @author zhangqianfeng
 *
 */
public class Field {
	public String toString(){
		return "表名："+tableName+"||  列名"+name+"||  列的类型："+type+"||  注释："+remarks+"||  主键："+primarKey+"||  是否递增："+autoinc+"||  是否为空："+allowNull;
		
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public boolean isPrimarKey() {
		return primarKey;
	}
	public void setPrimarKey(boolean primarKey) {
		this.primarKey = primarKey;
	}
	public boolean isAutoinc() {
		return autoinc;
	}
	public void setAutoinc(boolean autoinc) {
		this.autoinc = autoinc;
	}
	public boolean isAllowNull() {
		return allowNull;
	}
	public void setAllowNull(boolean allowNull) {
		this.allowNull = allowNull;
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getRemarks() {
		return remarks;
	}
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}
	
	private String tableName;
	private String name;
	private String type;
	private String remarks;
	private boolean primarKey;
	private boolean autoinc;
	private boolean allowNull;
	
	

	
	

}
