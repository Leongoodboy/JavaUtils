package com.seassoon.etl_tools.input;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Joiner;
import com.seassoon.etl.database.impl.BaseDatabase;
import com.seassoon.utils.LineReader;

import au.com.bytecode.opencsv.CSVParser;

public class CsvDistributedInputer extends FileInputer {
	private  Logger log = Logger.getLogger(CsvDistributedInputer.class);

	private String separator = ",";

	public CsvDistributedInputer setSeparator(String separator) {
		this.separator = separator;
		return this;
	}

	private long start;
	private long pos;
	private long end;
	private boolean isCompressedInput = false;
	private LongWritable key;
	private Text value;
	private int maxLineLength = Integer.MAX_VALUE;
	private String LF = "\n";
	private String escape = "\\";
	private String quote = "\"";

	// private ICsvListReader listReader = null;

	// private BufferedReader bufferedReader;

	public void setEscape(String escape) {
		this.escape = escape;
	}

	public void setQuote(String quote) {
		this.quote = quote;
	}

	private LineReader lineReader;

	private CSVParser csvParser;

	public CsvDistributedInputer(InputStream inputStream, String encoding) {
		super(inputStream, encoding);
	}

	public CsvDistributedInputer(String path, String encoding) throws FileNotFoundException {
		super(path, encoding);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void initialize() throws Exception {

		if(start==0){
			return;
		}
		// listReader = new CsvListReader(new InputStreamReader(inputStream,
		// encoding), CsvPreference.STANDARD_PREFERENCE);

		// bufferedReader = new BufferedReader(new
		// InputStreamReader(inputStream, encoding));
		lineReader = new LineReader(inputStream);

		csvParser = new CSVParser(separator.charAt(0),quote.charAt(0),escape.charAt(0));

		// this.pos = start;
		if (end == 0) {
			end = inputStream.available();
		}
		// end = inputStream.available();
		String nextLine;
		Text tempText=new Text();
		if (start != 0) {
			inputStream.skip(start);
			// pos += start;
		
			start += lineReader.readLine(tempText, Integer.MAX_VALUE, (int) Math.min(Integer.MAX_VALUE, end - pos));

		}
		this.pos = start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	@Override
	public String[] getHeader() throws Exception {
		if (lineReader == null) {
			this.initialize();
		}
		if (start != 0) {
			return null;
		}
		return super.getHeader();
	}

	private int maxBytesToConsume(long pos) {
		return isCompressedInput ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

	@Override
	public boolean read() {

		String nextLine;

		columns.clear();
		try {

			String[] result = null;

			if (key == null) {
				key = new LongWritable();
			}
			key.set(pos);
			if (value == null) {
				value = new Text();
			}
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

					newSize = lineReader.readLine(value, maxLineLength,
							Math.max(maxBytesToConsume(pos), maxLineLength));
					pos += newSize;
//					if (newSize < maxLineLength) {
//						break;
//					}

					if (!encoding.equals("UTF-8")) {
						nextLine = new String(value.getBytes(), 0, value.getLength(), encoding); 
					} else {
						nextLine = value.toString();
					}

//					if (nextLine == null || nextLine.contains("常州市川磷化工有限公司")) {
//						System.out.println(nextLine);
//					}

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
					// newSize = nextLine != null ?
					// nextLine.getBytes(encoding).length : 0;
					// pos += newSize;

					// tempValue = new Text();

					if ((newSize == 0)) {
						break;
					}

//					if (csvParser.isPending()) {
//						System.out.println(csvParser.isPending());
//					}

					if (csvParser.isPending()) {
						nextLine += LF;
					}

				} while (csvParser.isPending());

				// }

//				if ((newSize == 0)) {
//					//break;
//					 return false;
//				}
				break;
				// line too long. try again
				// LOG.info("Skipped line of size " + newSize + " at pos " +
				// (pos - newSize));
			}

			// data=csvParser.parseLine(nextLine);

			// List<String> data = listReader.read();
			if (result != null  && result.length!=1 && result[0] != "") {
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
			log.error(e);
		}

		return false;
	}

	public long getPos() {
		return pos;
	}

}
