package com.seassoon.etl_tools;

import java.io.OutputStream;

import com.seassoon.etl_tools.outout.CsvOutputer;
import com.seassoon.etl_tools.outout.ExcelXLSOutputer;
import com.seassoon.etl_tools.outout.ExcelXLSXOutputer;
import com.seassoon.etl_tools.outout.FileOutPuter;

public class OutputerFactory {

	public static FileOutPuter getOutputer(String format, OutputStream outputStream, String[] headerList) {

		format = format.toUpperCase();
		if (format.endsWith(FormatType.XLS.toString())) {

			return new ExcelXLSOutputer(outputStream, headerList);
		}

		else if (format.endsWith(FormatType.XLSX.toString())) {

			return new ExcelXLSXOutputer(outputStream, headerList);
		} else if (format.endsWith(FormatType.CSV.toString())) {

			return new CsvOutputer(outputStream, headerList);
		}

		return null;

	}

}
