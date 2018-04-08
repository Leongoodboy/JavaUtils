package com.seassoon.etl_tools.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
/**
 * 
 * @author zhangqianfeng
 *
 */
public class ExcelXLSXInputer extends FileInputer {



	private Workbook wb;

	private Sheet sheet; 

	private String sheetName;
	
	
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	private Queue<Sheet> sheetQueue=new LinkedBlockingQueue<>();
	
	private String[] sheets;

	public void setSheets(String[] sheets) {
		this.sheets = sheets;
	}

	private Integer sheetIndex;

	public ExcelXLSXInputer(InputStream inputStream) {
		super(inputStream);
	}


	public ExcelXLSXInputer(String path) throws FileNotFoundException {
		super(path);
	}

	@Override
	public void initialize() throws Exception {
		wb = new XSSFWorkbook(inputStream);



		if(sheets != null){
			for (String string : sheets) {
				Sheet sheet= wb.getSheet(string);
				if(sheet==null){
					throw new IOException("sheet不存在！");
				}
				sheetQueue .offer(sheet);
			}
		}
		
		
		if(sheetQueue.size()==0 && sheetName != null){
			Sheet sheet= wb.getSheet(sheetName);
			if(sheet==null){
				throw new IOException("sheet不存在！");
			}
			sheetQueue .offer(sheet);
		}
		
		if(sheetQueue.size()==0 && sheetIndex != null){
			Sheet sheet= wb.getSheetAt(sheetIndex);
			if(sheet==null){
				throw new IOException("sheet不存在！");
			}
			sheetQueue .offer(sheet);
		}
		
		if(sheetQueue.size()==0){

			sheetQueue .offer(wb.getSheetAt(0));
			
		}
		
		if(!nextSheet()){
			throw new IOException("sheet不存在！");
		}
		

	}
	
	
	@Override
	public String[] getHeader() throws Exception {
		if(wb ==null){
			this.initialize();
		}
		super.getHeader();

		if (lastHeader != null) {

			if (!Arrays.equals(header, lastHeader)) {
				throw new Exception("多sheet 列头数据不一致！");
			}
		}

		return header;
	}
	
	
	
	@Override
	public String[] next() throws Exception {
	

		if (header == null) {

			getHeader();

		}
		
		
		
		if (limit>0 && recordNumer == limit) {
			
			if(nextSheet() ){
				return  next();
			}
			return null;
		}
		
		
		if(queue.size()>0){
			recordNumer++;
			return queue.poll();
		}

		if (read()) {

			recordNumer++;
			return columns.toArray(new String[columns.size()]);
		}
		
		
		
		
		if(nextSheet() ){
			return  next();
		}

		return null;

	}
	
	public boolean nextSheet(){
		
		if(sheetQueue.size()>0){
			sheet= sheetQueue.poll();
			
			if(sheet != null){ 
				reset();
			}	
			return true;
		}
	
		
		return false;
		
		
		
	}

	@Override
	public boolean read() throws Exception {
		
	

		columns.clear();
		
		if(sheet == null){
			return false;
		}

		Row row = sheet.getRow(rowNumber);

		if (row != null) {
			
			int columnsSize= row.getLastCellNum();
			
			if(header != null){
				
//				if(columnsSize != header.length){
//					throw new Exception("列数量不一致"); 
//				}
				
				columnsSize=header.length;
			}
			
			
			

			for (int i = 0; i < columnsSize; i++) {

				if (row.getCell(i) != null) {
					columns.add(getCellValue(row.getCell(i) ));
				} else {
					columns.add(null);
				}

			}
			rowNumber++;
			return true;
		}

		return false;
	}
	

	/**
	 * 获取单元格的值
	 * 
	 * @param hcell
	 * @return
	 */
	private String getCellValue(Cell hcell) {

		String value = null;
		if (null != hcell) {
			switch (hcell.getCellType()) {
			// 单元格是函数计算出来的数据
			case HSSFCell.CELL_TYPE_FORMULA:
				try {
					value = String.valueOf(hcell.getNumericCellValue());
				} catch (Exception e) {
					value = String.valueOf(hcell.getRichStringCellValue());
				}
				break;
			// 单元格是数字类型的
			case HSSFCell.CELL_TYPE_NUMERIC:
				// 获取单元格的样式值，即获取单元格格式对应的数值
				int style = hcell.getCellStyle().getDataFormat();
				// 判断是否是日期格式
				if (HSSFDateUtil.isCellDateFormatted(hcell)) {
					Date date = hcell.getDateCellValue();
					// 对不同格式的日期类型做不同的输出，与单元格格式保持一致
					switch (style) {
					case 178:
						value = new SimpleDateFormat("yyyy'年'M'月'd'日'").format(date);
						break;
					case 14:
						value = new SimpleDateFormat("yyyy/MM/dd").format(date);
						break;
					case 179:
						value = new SimpleDateFormat("yyyy/MM/dd HH:mm").format(date);
						break;
					case 181:
						value = new SimpleDateFormat("yyyy/MM/dd HH:mm a ").format(date);
						break;
					case 22:
						value = new SimpleDateFormat(" yyyy/MM/dd HH:mm:ss ").format(date);
						break;
					default:
						break;
					}
				} else {
//					switch (style) {
//					// 单元格格式为百分比，不格式化会直接以小数输出
//					case 9:
//						value = new DecimalFormat("0.00%").format(hcell.getNumericCellValue());
//						break;
//					// DateUtil判断其不是日期格式，在这里也可以设置其输出类型
//					case 57:
//						value = new SimpleDateFormat(" yyyy'年'MM'月' ").format(hcell.getDateCellValue());
//						break;
//					default:
//						value = String.valueOf(hcell.getNumericCellValue());
//						break;
//					}
					DecimalFormat df = new DecimalFormat("0");   
					value = hcell!=null?  df.format(hcell.getNumericCellValue()):null;
					//System.out.println("数值"+value);
					break;
				}

				break;
			// 单元格是字符串类型的
			case HSSFCell.CELL_TYPE_STRING:
				value = String.valueOf(hcell.getRichStringCellValue());
				break;
			default:
				value =  hcell!=null? hcell.toString():null;
				

			}
		}
		return value;
	}
	
	

}
