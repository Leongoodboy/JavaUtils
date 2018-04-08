package api;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class 测试 {
	public static Reader getReader(String relativePath) throws UnsupportedEncodingException, FileNotFoundException {
	
		return new InputStreamReader(new FileInputStream(relativePath), "UTF-8");
	
	}
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

//		CsvInputer inputer = new CsvInputer("C:\\Users\\zhangqianfeng\\Downloads\\test1228.csv", "UTF-8");
//
//		String folder = System.getProperty("java.io.tmpdir");
//
//		String[] line = null;
//		int count = 0;
//		try {
//			while ((line = inputer.next()) != null) {
//				count++;
//				System.out.println(count + " " + line[0]);
//
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		//CsvParser parser = new CsvParser(new CsvParserSettings());
		//List<String[]> allRows = parser.parseAll(getReader("C:\\Users\\zhangqianfeng\\Downloads\\user.csv"));

		// creates a CSV parser
		CsvParserSettings settings = new CsvParserSettings();
		//the file used in the example uses '\n' as the line separator sequence.
		//the line separator sequence is defined here to ensure systems such as MacOS and Windows
		//are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
		settings.getFormat().setLineSeparator("\n");
		CsvParser parser = new CsvParser(settings);
		
		// call beginParsing to read records one by one, iterator-style.
		parser.beginParsing(getReader("C:\\Users\\zhangqianfeng\\Downloads\\test1228 (1).csv"));
		
		String[] row;
		while ((row = parser.parseNext()) != null) {
			System.out.println(row[0]);
		}
		
		// The resources are closed automatically when the end of the input is reached,
		// or when an error happens, but you can call stopParsing() at any time.
		
		// You only need to use this if you are not parsing the entire content.
		// But it doesn't hurt if you call it anyway.
		parser.stopParsing();

	}
}
