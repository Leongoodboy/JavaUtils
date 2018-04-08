package com.seassoon.etl_tools;

import java.io.FileNotFoundException;
import java.io.InputStream;

import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.input.ExcelXLSInputer;
import com.seassoon.etl_tools.input.ExcelXLSXInputer;
import com.seassoon.etl_tools.input.ExcelXLSXInputerSax;
import com.seassoon.etl_tools.input.FileInputer;

public class InputerFactory {
	
	public static FileInputer getFileInputer(String format, InputStream inputStream) {

		format = format.toUpperCase();
		if (format.endsWith(FormatType.XLS.toString())) {

			return new ExcelXLSInputer(inputStream);
		}

		else if (format.endsWith(FormatType.XLSX.toString())) {

			//return new ExcelXLSXInputer(inputStream);
			
			return new ExcelXLSXInputer(inputStream);
			
		} else if (format.endsWith(FormatType.CSV.toString())) {

			return new CsvInputer(inputStream);
		}

		return null;

	}
	

	public static FileInputer getFileInputer(String format, InputStream inputStream,String encoding) {

		format = format.toUpperCase();
		if (format.endsWith(FormatType.XLS.toString())) {

			return new ExcelXLSInputer(inputStream);
		}

		else if (format.endsWith(FormatType.XLSX.toString())) {

			//return new ExcelXLSXInputer(inputStream);
			
			return new ExcelXLSXInputer(inputStream);
			
		} else if (format.endsWith(FormatType.CSV.toString())) {

			return new CsvInputer(inputStream,encoding);
		}

		return null;

	}
	
	public static ExcelXLSXInputerSax getExcelXLSX(String format, InputStream inputStream) throws FileNotFoundException{
		return new ExcelXLSXInputerSax(inputStream);
	}

}
