package com.seassoon.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelUtils {

	public static String getType(String path) {

		String suffix = path.substring(path.indexOf(".") + 1, path.length());
		if (suffix.equals("xls")) {
			return "xls";
		}
		if (suffix.equals("xlsx")) {
			return "xlsx";
		}
		return null;

	}

	public static Workbook getWorkBook(String format, InputStream inputStream) throws Exception {
		Workbook wb = null;
		if ("XLS".equals(format.toUpperCase())) {
			wb = new HSSFWorkbook(inputStream);
		} else if ("XLSX".equals(format.toUpperCase())) {
			wb = new XSSFWorkbook(inputStream);
		} else {
			throw new Exception("当前文件不是excel文件");
		}
		return wb;

	}
	
	public static Workbook getWriteWorkBook(String format) throws Exception {
		Workbook wb = null;
		if ("XLS".equals(format.toUpperCase())) {
			wb = new HSSFWorkbook();
		} else if ("XLSX".equals(format.toUpperCase())) {
			wb = new XSSFWorkbook();
		} else {
			throw new Exception("当前文件不是excel文件");
		}
		return wb;

	}

	public static List<Map<String, String>> getSheetNames(String type, InputStream inputStream) {

		List<Map<String, String>> sheetNames = new ArrayList<>();

		Workbook wb = null;
		try {
			if ("xls".equals(type)) {
				wb = new HSSFWorkbook(inputStream);
			} else if ("xlsx".equals(type)) {
				wb = new XSSFWorkbook(inputStream);
			} else {
				throw new Exception("当前文件不是excel文件");
			}

			for (Sheet sheet : wb) {
				Map<String, String> map = new HashMap();
				map.put("sheetName", sheet.getSheetName());
				map.put("rows", String.valueOf(sheet.getLastRowNum()));
				sheetNames.add(map);
				// sheetNames.add( new HashMap<>() new String[]{
				// sheet.getSheetName(), String.valueOf(sheet.getLastRowNum())
				// });
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (wb != null) {
				try {
					wb.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return sheetNames;

	}

}
