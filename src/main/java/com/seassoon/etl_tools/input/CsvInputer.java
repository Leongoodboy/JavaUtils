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

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class CsvInputer extends FileInputer {

	private String delimiter = ",";
	private String quote = "\"";



	public void setQuote(String quote) {
		this.quote = quote;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}



//	private ICsvListReader listReader = null;
	private CsvParser parser=null;

	public CsvInputer(InputStream inputStream) {
		super(inputStream);
	}
	public CsvInputer(InputStream inputStream, String encoding) {
		super(inputStream,encoding);
	}

	public CsvInputer(String path, String encoding) throws FileNotFoundException {
		super(path, encoding);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void initialize() throws Exception {
		
		CsvParserSettings settings = new CsvParserSettings();

		settings.getFormat().setLineSeparator("\n");
		settings.getFormat().setDelimiter( delimiter.charAt(0));
		settings.getFormat().setQuote( quote.charAt(0));
		settings.getFormat().setQuoteEscape('\\');
		settings.setMaxCharsPerColumn(1000000);
		
		 parser = new CsvParser(settings);
		
		// call beginParsing to read records one by one, iterator-style.
		parser.beginParsing(new BufferedReader(new InputStreamReader(inputStream, encoding)));
		
	

//		listReader = new CsvListReader(new BufferedReader(new InputStreamReader(inputStream, encoding)),
//				new CsvPreference.Builder(quote.charAt(0), delimiter.charAt(0), "\r\n").build());

	}

	@Override
	public String[] getHeader() throws Exception {
		if (parser == null) {
			this.initialize();
		}
		return super.getHeader();
	}

	@Override
	public boolean read() throws Exception {

		columns.clear();
		String[] dataArray=parser.parseNext();

		if (dataArray != null) {
			List<String> data =Arrays.asList(dataArray);
			int columnsSize = data.size();

			if (header != null) {

				// if(columnsSize != header.length){
				// throw new Exception("列数量不一致");
				// }

				columnsSize = header.length;
			}

			for (int i = 0; i < columnsSize; i++) {

				if (i < data.size() && data.get(i) != null) {
					columns.add(data.get(i).toString());
				} else {
					columns.add(null);
				}

			}

			rowNumber++;
			return true;
		}

		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		super.close();
	
		parser.stopParsing();
	}

	
	
	
}
