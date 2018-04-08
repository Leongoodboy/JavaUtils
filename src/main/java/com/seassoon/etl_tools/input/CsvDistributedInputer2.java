package com.seassoon.etl_tools.input;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Joiner;

import au.com.bytecode.opencsv.CSVParser;

public class CsvDistributedInputer2 extends FileInputer {

	private String separator = ",";

	public CsvDistributedInputer2 setSeparator(String separator) {
		this.separator = separator;
		return this;
	}

	private long start;
	private long pos;
	private long end;

	private String LF = "\n";
	private String escape = "\\";
	private String quote = "\"";

	// private ICsvListReader listReader = null;

	private BufferedReader bufferedReader;

	private CSVParser csvParser;

	public CsvDistributedInputer2(InputStream inputStream, String encoding) {
		super(inputStream, encoding);
	}

	public CsvDistributedInputer2(String path, String encoding) throws FileNotFoundException {
		super(path, encoding);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void initialize() throws Exception {

		// listReader = new CsvListReader(new InputStreamReader(inputStream,
		// encoding), CsvPreference.STANDARD_PREFERENCE);

		bufferedReader = new BufferedReader(new InputStreamReader(inputStream, encoding));

		csvParser = new CSVParser();

		// this.pos = start;
		if (end == 0) {
			end = inputStream.available();
		}
		// end = inputStream.available();
		String nextLine;
		if (start != 0) {
			inputStream.skip(start);
			pos += start;
			nextLine = bufferedReader.readLine();
			pos += nextLine.getBytes(encoding).length;
		}

	}

	public void setStart(long start) {
		this.start = start;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	@Override
	public String[] getHeader() throws Exception {
		if (bufferedReader == null) {
			this.initialize();
		}
		return super.getHeader();
	}

	@Override
	public boolean read() {

		String nextLine;

		columns.clear();
		try {

			String[] result = null;
			int newSize = 0;
			while (pos <= end) {
				// if (pos == 0) {
				// newSize = skipUtfByteOrderMark();
				// } else {
				// newSize = in.readLine(value, maxLineLength,
				// maxBytesToConsume(pos));
				// pos += newSize;
				// csv 间隔处理
				do {
					// newSize = in.readLine(tempValue, maxLineLength,
					// Math.max(maxBytesToConsume(pos), maxLineLength));

					nextLine = bufferedReader.readLine();
					if (nextLine == null || nextLine.contains("常州市川磷化工有限公司")) {
						System.out.println(nextLine);
					}

					if (nextLine != null) {
						/**
						 * 处理特殊 封闭符 问题
						 */
						nextLine = nextLine.replace(
								String.valueOf(CSVParser.DEFAULT_ESCAPE_CHARACTER)
										+ String.valueOf(CSVParser.DEFAULT_QUOTE_CHARACTER) + separator,
								String.valueOf(CSVParser.DEFAULT_QUOTE_CHARACTER) + separator);

						if (nextLine.lastIndexOf(String.valueOf(CSVParser.DEFAULT_ESCAPE_CHARACTER)
								+ String.valueOf(CSVParser.DEFAULT_QUOTE_CHARACTER)) == nextLine.length() - 2) {
							nextLine = nextLine.replace(
									String.valueOf(CSVParser.DEFAULT_ESCAPE_CHARACTER)
											+ String.valueOf(CSVParser.DEFAULT_QUOTE_CHARACTER),
									String.valueOf(CSVParser.DEFAULT_QUOTE_CHARACTER));
						}
						// String nextLine = new String(tempValue.getBytes(), 0,
						// tempValue.getLength(), "UTF-8");

						// nextLine = nextLine.replace(escape + quote + new
						// String(recordSeparatorBytes), quote + new
						// String(recordSeparatorBytes));

						// if(nextLine.lastIndexOf(escape + quote ) ==
						// nextLine.length()-2){
						// nextLine = nextLine.replace(escape + quote , quote );
						// }

						// value.append(tempValue.getBytes(), 0,
						// tempValue.getLength());

						String[] r = csvParser.parseLineMulti(nextLine);
						if (nextLine != null && r.length > 0) {
							if (result == null) {
								result = r;
							} else {
								String[] t = new String[result.length + r.length];
								System.arraycopy(result, 0, t, 0, result.length);
								System.arraycopy(r, 0, t, result.length, r.length);
								result = t;
							}
						}
					}
					newSize = nextLine != null ? nextLine.getBytes(encoding).length : 0;
					pos += newSize;

					// tempValue = new Text();

					if ((newSize == 0)) {
						break;
					}

					if (csvParser.isPending()) {
						System.out.println(csvParser.isPending());
					}

					if (csvParser.isPending()) {
						nextLine += LF;
					}

				} while (csvParser.isPending());

				// }

				if ((newSize == 0)) {
					break;
				}
				break;
				// line too long. try again
				// LOG.info("Skipped line of size " + newSize + " at pos " +
				// (pos - newSize));
			}

			// data=csvParser.parseLine(nextLine);

			// List<String> data = listReader.read();
			if (result != null) {
				int columnsSize = result.length;

				if (header != null) {

					// if(columnsSize != header.length){
					// throw new Exception("列数量不一致");
					// }

					columnsSize = header.length;
				}

				for (int i = 0; i < columnsSize; i++) {

					if (i < result.length && result[i] != null) {
						columns.add(result[i].toString());
					} else {
						columns.add(null);
					}

				}
				rowNumber++;
				return true;
			} else {
				System.out.println(end);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	public long getPos() {
		return pos;
	}

}
