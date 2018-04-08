package com.seassoon.etl_tools.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class FileInputer extends AbstractInputer {

	private Boolean isFirstHeader = true;

	protected ConcurrentLinkedQueue<String[]> queue = new ConcurrentLinkedQueue<>();

	/**
	 * 用于生成空列
	 */
	private Integer empty_header_index = 0;

	/**
	 * 读取数量
	 */
	protected Integer limit = 0;

	/**
	 * 输入流
	 */
	protected InputStream inputStream;

	/**
	 * 文件路径
	 */
	protected String path;

	protected String encoding = "UTF-8";

	public void setIsFirstHeader(Boolean isFirstHeader) {
		this.isFirstHeader = isFirstHeader;
	}

	public String getEncoding() {
		return encoding;
	}

	public FileInputer setEncoding(String encoding) {
		this.encoding = encoding;
		return this;
	}

	public Integer getLimit() {
		return limit;
	}

	public FileInputer setLimit(Integer limit) {
		this.limit = limit;
		return this;
	}

	public FileInputer(InputStream inputStream) {
		super();
		this.inputStream = inputStream;

	}

	public FileInputer(String path) throws FileNotFoundException {
		super();
		this.inputStream = new FileInputStream(new File(path));

	}

	public FileInputer(InputStream inputStream, String encoding) {
		super();
		this.inputStream = inputStream;
		this.encoding = encoding;

	}

	public FileInputer(String path, String encoding) throws FileNotFoundException {
		super();
		this.inputStream = new FileInputStream(new File(path));
		this.encoding = encoding;

	}

	@Override
	public String[] next() throws Exception {

		if (header == null) {

			getHeader();

		}

		if (limit > 0 && recordNumer == limit) {
			return null;
		}

		if (queue.size() > 0) {
			recordNumer++;
			return queue.poll();
		}

		if (read()) {

			recordNumer++;
			return columns.toArray(new String[columns.size()]);
		}

		return null;

	}

	@Override
	public String[] getHeader() throws Exception {

		if (header == null) {

			if (read()) {
				// 省略行

				// header = new ArrayList<>();
				if (isFirstHeader == true) {
					header = columns.toArray(new String[columns.size()]);
				} else {
					header = columns.toArray(new String[columns.size()]);
					for (int i = 0; i < header.length; i++) {
						String string = header[i];

						
						empty_header_index++;
						string = "_c" + empty_header_index;
				 
						header[i] = string;
				

					}
					queue.add(columns.toArray(new String[columns.size()]));
				}

			}

		}

		return header;

	}

	/**
	 * 读取准备操作
	 * 
	 * @throws Exception
	 */
	private void ready_next() throws Exception {

		// // 忽略行
		// igoneLine();
		// // 列头
		// headerLine();
		//
		// // 跳过行
		// skipDataLine();

	}

	// public void headerLine() throws Exception {
	// if (headerLines > 0 && headerLines_num < headerLines) {
	// if (read()) {
	// // 省略行
	// if (header == null) {
	// // header = new ArrayList<>();
	// header = columns.toArray(new String[columns.size()]);
	// // header.addAll(columns);
	// } else {
	//
	// for (int i = 0; i < columns.size(); i++) {
	//
	// if (i < header.length) {
	// // header.set(i, header.get(i) + " " +
	// // columns.get(i));
	//
	// header[i] = header[i] + " " + columns.get(i);
	// } else {
	// // header[i] = header[i] + " " +
	// // columns.get(i);
	// // header.add(columns.get(i));
	// throw new Exception("超出列数量！");
	// }
	//
	// }
	// }
	//
	// headerLines_num++;
	// headerLine();
	//
	// }
	//
	// } else {
	//
	// validateColumnsName();
	// }
	//
	//
	//
	// }

	/**
	 * 验证列名
	 */
	private void validateColumnsName() {

		for (int i = 0; i < header.length; i++) {
			String string = header[i];

			if (string == null || string.trim().equals("")) {
				empty_header_index++;
				string = "列" + empty_header_index;
				// header.set(i, string);
				header[i] = string;
			}

		}

	}

	// public void igoneLine() throws Exception {
	//
	// if (ignoreLines > 0 && ignoreLines_num < ignoreLines) {
	// if (read()) {
	// // 省略行
	//
	// ignoreLines_num++;
	// igoneLine();
	//
	// }
	// }
	//
	// }

	// public void skipDataLine() throws Exception {
	// if (skipDataLines > 0 && skipDataLines_num < skipDataLines) {
	// if (read()) {
	// // 跳过行
	// skipDataLines_num++;
	// skipDataLine();
	// }
	// }
	//
	// }

	public void reset() {

		// headerLines_num = 0;
		//
		// ignoreLines_num = 0;
		//
		// skipDataLines_num = 0;

		// limit_num = 0;

		lastHeader = header;

		recordNumer = 0;
		rowNumber = 0;
		empty_header_index = 0;

		header = null;

	}

	public void close() {
		// TODO Auto-generated method stub
		try {
			inputStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
