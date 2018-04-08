package com.seassoon.etl_tools.outout;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.google.common.base.Joiner;
import com.seassoon.etl_tools.FormatType;
import com.seassoon.etl_tools.input.FileInputer;
import com.seassoon.etl_tools.outout.OutPuter;
import com.seassoon.utils.ExcelUtils;

public class ExcelXLSOutputer extends FileOutPuter {

	public BlockingQueue<String[]> queue;

	private static Logger log = Logger.getLogger(ExcelXLSOutputer.class);

	private String filePath;

	private Integer submitSize = 10000;

	private List<String[]> list = new ArrayList<>();

	Workbook workbook;
	Sheet writeSheet;
	Row writeRow;

	boolean is_print_head = false;

	public ExcelXLSOutputer(OutputStream outputStream, String[] headerList) {
		super(outputStream, headerList);
		// TODO Auto-generated constructor stub
	}

	public ExcelXLSOutputer(String path, String[] headerList) throws IOException {
		super(path, headerList);
		// TODO Auto-generated constructor stub
	}

	public void initialize() throws Exception {

		workbook = ExcelUtils.getWriteWorkBook(FormatType.XLS.toString());

		writeSheet = workbook.createSheet();

		super.initialize();

	}

	public void process(String[] data) throws Exception {
		
		if (workbook == null) {
			this.initialize();
		}

		writeRow = writeSheet.createRow(processNum.intValue());

		processNum.incrementAndGet();

		// list.add(data);
		// if (list.size() > submitSize) {
		// execute();
		// }

		for (int i = 0; i < data.length; i++) {
			if(data[i]!=null && data[i].length()>32767){
				writeRow.createCell(i).setCellValue(data[i].substring(0, 32767));
			}else{
				writeRow.createCell(i).setCellValue(data[i]);
			}
			
		}

	}

	String tempResult = null;

	public String convertData(String[] nextLine) {
		// csvWriter.writeNext(nextLine);
		//
		// tempResult= stringWriter.toString();
		// stringWriter.flush();

		return "\"" + Joiner.on("\",\"").useForNull("").join(nextLine) + "\"\n";
	}

	public void close() {

		try {
			// this.csvWriter.close();
			// this.bufferedWriter.flush();
			workbook.write(outputStream);
			workbook.close();
			this.outputStream.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
