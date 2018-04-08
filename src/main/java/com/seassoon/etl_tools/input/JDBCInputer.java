package com.seassoon.etl_tools.input;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.seassoon.etl.database.DataBaseFactory;
import com.seassoon.etl.database.DatabaseInterface;
import com.seassoon.utils.ConnectionDB;

import sun.misc.BASE64Encoder;

public class JDBCInputer extends AbstractInputer {

	// public String table

	// public BlockingQueue<String[]> queue;

	public String sql;

	private String dbType;

	private DatabaseInterface databaseInterface;

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

	private static Logger log = Logger.getLogger(JDBCInputer.class);

	// 执行SQL获得结果集
	protected ResultSet rs;

	public JDBCInputer(String sql, String URL, String USERNAME, String PASSWORD, String dbType) {

		super();
		this.sql = sql;
		// this.db = db;

		this.URL = URL;
		this.USERNAME = USERNAME;
		this.PASSWORD = PASSWORD;
		this.dbType = dbType;

	}

	public JDBCInputer(String sql, DatabaseInterface databaseInterface) {

		super();
		this.sql = sql;
		// this.db = db;

		this.databaseInterface = databaseInterface;

	}

	protected ResultSetMetaData rsmd;

	@Override
	public void initialize() throws Exception {

		// db = new ConnectionDB(URL, USERNAME, PASSWORD, DRIVER);

		if (databaseInterface == null) {
			databaseInterface = DataBaseFactory.getDatabaseInterface(dbType, URL, USERNAME, PASSWORD);
		}

		rs = databaseInterface.query(sql, null);

		rsmd = rs.getMetaData();

		super.initialize();

	}

	@Override
	public String[] getHeader() throws Exception {

		if (rs == null) {
			this.initialize();
		}

		if (header == null) {

			header = new String[rsmd.getColumnCount()];
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {

				header[i - 1] = rsmd.getColumnLabel(i);

			}
		}

		return header;
	}

	@Override
	public String[] next() throws SQLException, IOException {

		if (read()) {
			recordNumer++;
			return columns.toArray(new String[columns.size()]);
		}

		close();

		return null;

	}

	@Override
	public void close() {
		databaseInterface.closeAll();

	}

	@Override
	public boolean read() throws SQLException, IOException {
		columns.clear();
		// try {
		if (rs.next()) {

			for (int i = 1; i <= rsmd.getColumnCount(); i++) {

				// try {
				if (rsmd.getColumnType(i) == Types.CLOB)
					columns.add(clobToString(rs.getClob(i)));
				else if (rsmd.getColumnType(i) == Types.BLOB)
					columns.add( blobToString(rs.getBlob(i)));
				else
					columns.add( rs.getString(i));

				//columns.add(rs.getString(i));
				// } catch (SQLException e) {
				// columns.add(null);
				// }

			}
			if (!columns.isEmpty()) {
				rowNumber++;
				return true;
			}
		}

		// } catch (SQLException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		return false;
	}

	/**
	 * 将"Clob"型数据转换成"String"型数据 需要捕获"SQLException","IOException" prama: colb1
	 * 将被转换的"Clob"型数据 return: 返回转好的字符串
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	private static String clobToString(Clob colb) throws SQLException, IOException {
		String outfile = "";
		if (colb != null) {
			// oracle.sql.CLOB clob = (oracle.sql.CLOB)colb1;
			java.io.Reader is = colb.getCharacterStream();
			java.io.BufferedReader br = new java.io.BufferedReader(is);
			String s = br.readLine();
			while (s != null) {
				outfile += s;
				s = br.readLine();
			}
			is.close();
			br.close();
		}
		return outfile;
	}

	/**
	 * 将"Clob"型数据转换成"String"型数据 需要捕获"SQLException","IOException" prama: colb1
	 * 将被转换的"Clob"型数据 return: 返回转好的字符串
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	private static String blobToString(Blob blob) throws SQLException, IOException {
		// Blob 是二进制文件，转成文字是没有意义的
		// 所以根据传输协议，EzManager的传输协议是无法支持的
		byte[] base64;
		String newStr = ""; // 返回字符串

		if (blob != null) {
			try {
				base64 = org.apache.commons.io.IOUtils.toByteArray(blob.getBinaryStream());
				newStr = new BASE64Encoder().encodeBuffer(base64);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return newStr;
	}

}
