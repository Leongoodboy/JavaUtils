package com.seassoon.etl_tools.outout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.outout.OutPuter;

import au.com.bytecode.opencsv.CSVWriter;

public class CsvOutputer extends FileOutPuter {

	public CsvOutputer(OutputStream outputStream, String[] headerList) {
		super(outputStream, headerList);
		// TODO Auto-generated constructor stub
	}


	public CsvOutputer(String path, String[] headerList) throws IOException {
		super(path, headerList);
		// TODO Auto-generated constructor stub
	}


	private static Logger log = Logger.getLogger(CsvOutputer.class);

	private Integer submitSize = 10000;

	// CSVWriter csvWriter;

	BufferedWriter bufferedWriter;

	StringWriter stringWriter;

	private String separator = ",";

	public CsvOutputer setSeparator(String separator) {
		this.separator = separator;
		return this;
	}

	boolean is_print_head = false;

	


	public void initialize() throws Exception {

		// writer = new FileWriter(filePath);
		stringWriter = new StringWriter();
		// csvWriter = new CSVWriter(stringWriter);

		bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
		
		super.initialize();
		
	}

	@Override
	public void process(String[] data) throws Exception {
		
		if (bufferedWriter == null) {
			this.initialize();
		}
		if(data==null){
			return;
		}
		processNum.incrementAndGet();
		bufferedWriter.write(convertData(data));
		

	}



	String tempResult = null;

	public String convertData(String[] nextLine) {
		// csvWriter.writeNext(nextLine);
		//
		// tempResult= stringWriter.toString();
		// stringWriter.flush();
		for (int i = 0; i < nextLine.length; i++) {
			if(nextLine[i] != null){
				//nextLine[i].replaceAll("\s", replacement)
				nextLine[i] =nextLine[i].replaceAll("\"\"", "\\\"").replaceAll("\n", "\\\\n").replaceAll("\r", "\\\\r");

				
			} 
	
		}
		
		return "\"" + Joiner.on("\""+separator+"\"").useForNull("").join(nextLine) + "\"\n";
	}

	public void close() {
	
		try {
		
			if(bufferedWriter != null){
				//this.bufferedWriter.flush();
				this.bufferedWriter.close();
			}
			super.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	

}
