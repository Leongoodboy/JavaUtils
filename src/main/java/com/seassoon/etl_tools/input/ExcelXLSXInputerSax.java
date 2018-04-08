package com.seassoon.etl_tools.input;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import com.seassoon.etl_tools.outout.OutPuter;

public class ExcelXLSXInputerSax extends DefaultHandler implements Inputer {
	// 共享字符串表
	private SharedStringsTable sst;

	// 上一次的内容
	private String lastContents;
	private boolean nextIsString;
	protected String[] header = null;
	private int sheetIndex = -1;
	private List<String> rowList = new ArrayList<String>();

	// 当前行
	private int curRow = 0;
	// 当前列
	private int curCol = 0;
	// 日期标志
	private boolean dateFlag;
	// 数字标志
	private boolean numberFlag;

	private boolean isTElement;

	// 我开始添加的
	protected InputStream inputStream;
	private InputStream sheet;
	private XMLReader parser;
	private Iterator<InputStream> sheets;
	private XSSFReader r;
	// 是否是列头
	private Boolean isFirstHeader = true;
	private LinkedBlockingQueue<String[]> rowQueue = new LinkedBlockingQueue<>(200);
	private String[] headerNext;
	private String[] data;
	private static Boolean flagEnd = false;

	public Boolean getIsFirstHeader() {
		return isFirstHeader;
	}

	public void setIsFirstHeader(Boolean isFirstHeader) {
		this.isFirstHeader = isFirstHeader;
	}

	public ExcelXLSXInputerSax(InputStream inputStream2) throws FileNotFoundException {
		super();
		this.inputStream = inputStream2;

	}

	@Override
	public void initialize() throws Exception {

		new Thread("insert_data") {
			public void run() {

				try {
					OPCPackage pkg = OPCPackage.open(inputStream);
					r = new XSSFReader(pkg);
					SharedStringsTable sst = r.getSharedStringsTable();
					parser = fetchSheetParser(sst);
					sheets = r.getSheetsData();
					while (sheets.hasNext()) {
						curRow = 0;
						sheetIndex++;
						sheet = sheets.next();
						InputSource sheetSource = new InputSource(sheet);
						parser.parse(sheetSource);
						sheet.close();
					}
					flagEnd = true;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}.start();

	}

	@Override
	public String[] getHeader() throws Exception {

		if (null == parser) {
			this.initialize();
		}
		while (true) {
			if (!rowQueue.isEmpty() && isFirstHeader) {
				return header;
			}
		}

	}

	@Override
	public String[] next() throws Exception {
		if (null == parser) {
			this.initialize();
		}
		while(true){
			// 不为空 ， 开始
			if (!rowQueue.isEmpty() ) {
				return rowQueue.poll();
			}
			// 为空 ， 开始 ,没结束
			if (rowQueue.isEmpty() && flagEnd) {
				return null;
			}
		}
		// 不为空 ， 开始
//		if (!rowQueue.isEmpty()&& flagStart) {
//			return rowQueue.poll();
//		}
//		// 为空 ， 开始 ,没结束
//		if (rowQueue.isEmpty() && flagStart && !flagEnd) {
//			return next();
//		}
//		// 为空 ，开始， 结束
//		if (rowQueue.isEmpty() && flagStart && flagEnd) {
//			System.out.println("返回数据null" + flagStart);
//			return null;
//		}
//		//队列为空， 
//		return next();
	}

	@Override
	public void close() throws Exception {
		sheet.close();
	}

	@Override
	public void out(OutPuter outPuter) throws Exception {
		String[] temp = null;
		while ((temp = this.next()) != null) {
			data = temp;
			outPuter.process(data);
			// System.out.println(recordNumer);
		}
		outPuter.close();
	}

	public void endElement(String uri, String localName, String name) throws SAXException {

		// System.out.println("endElement: " + localName + ", " + name);

		// 根据SST的索引值的到单元格的真正要存储的字符串
		// 这时characters()方法可能会被调用多次
		if (nextIsString) {
			try {
				int idx = Integer.parseInt(lastContents);
				lastContents = new XSSFRichTextString(sst.getEntryAt(idx)).toString();
			} catch (Exception e) {

			}
		}
		// t元素也包含字符串
		if (isTElement) {
			String value = lastContents.trim();
			rowList.add(curCol, value);
			curCol++;
			isTElement = false;
			// v => 单元格的值，如果单元格是字符串则v标签的值为该字符串在SST中的索引
			// 将单元格内容加入rowlist中，在这之前先去掉字符串前后的空白符
		} else if ("v".equals(name)) {
			String value = lastContents.trim();
			value = value.equals("") ? " " : value;
			try {
				// 日期格式处理
				if (dateFlag) {
					Date date = HSSFDateUtil.getJavaDate(Double.valueOf(value));
					SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
					value = dateFormat.format(date);
				}
				// 数字类型处理
				if (numberFlag) {
					BigDecimal bd = new BigDecimal(value);
					value = bd.setScale(3, BigDecimal.ROUND_UP).toString();
				}
			} catch (Exception e) {
				// 转换失败仍用读出来的值
			}
			rowList.add(curCol, value);
			curCol++;
		} else {
			// 如果标签名称为 row ，这说明已到行尾，调用 optRows() 方法
			if (name.equals("row")) {

				if (isFirstHeader == true && curRow == 0) {

					if (null == header) {
						header = rowList.toArray(new String[rowList.size()]);
					} else if (null != header) {
						headerNext = rowList.toArray(new String[rowList.size()]);
						if (!Arrays.equals(header, headerNext)) {
							try {
								throw new Exception("第" + sheetIndex + 1 + "个sheet 列头数据不一致！");
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}

				} else {
					try {
						rowQueue.put(rowList.toArray(new String[rowList.size()]));
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

				rowList.clear();
				curRow++;
				curCol = 0;
			}
		}

	}

	public XMLReader fetchSheetParser(SharedStringsTable sst) throws SAXException {
		XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
		this.sst = sst;
		parser.setContentHandler(this);
		return parser;
	}

	public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {

		// System.out.println("startElement: " + localName + ", " + name + ", "
		// + attributes);

		// c => 单元格
		if ("c".equals(name)) {
			// 如果下一个元素是 SST 的索引，则将nextIsString标记为true
			String cellType = attributes.getValue("t");
			if ("s".equals(cellType)) {
				nextIsString = true;
			} else {
				nextIsString = false;
			}
			// 日期格式
			String cellDateType = attributes.getValue("s");
			if ("1".equals(cellDateType)) {
				dateFlag = true;
			} else {
				dateFlag = false;
			}
			String cellNumberType = attributes.getValue("s");
			if ("2".equals(cellNumberType)) {
				numberFlag = true;
			} else {
				numberFlag = false;
			}

		}
		// 当元素为t时
		if ("t".equals(name)) {
			isTElement = true;
		} else {
			isTElement = false;
		}

		// 置空
		lastContents = "";
	}

	public void characters(char[] ch, int start, int length) throws SAXException {
		// 得到单元格内容的值
		lastContents += new String(ch, start, length);
	}

	/**
	 * 获取行数据回调
	 * 
	 * @param sheetIndex
	 * @param curRow
	 * @param rowList
	 * @throws Exception
	 * @throws IOException
	 */
	// public abstract void getRows(int sheetIndex, int curRow, List<String>
	// rowList);
}
