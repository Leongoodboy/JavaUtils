package etl_tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import com.google.common.base.Joiner;

import au.com.bytecode.opencsv.CSVReader;

public class IOTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		
		InputStream inputStream=new FileInputStream("C:\\Users\\zhangqianfeng\\Downloads\\alcdiffdtl.csv");
		
		inputStream.skip(10);
		
		BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(inputStream,"gbk"));
		
		System.out.println(bufferedReader.readLine());
//		System.out.println(bufferedReader.readLine());
		
//		String data;
//		while ((data  = bufferedReader.readLine()) != null) {
//			System.out.println(data);
//		}
		System.out.println(inputStream.markSupported());

		BufferedReader bufferedReader2=new BufferedReader(new InputStreamReader(inputStream,"gbk"));
		
		System.out.println(bufferedReader2.readLine());
		
		 String[] strs ;
//		CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream,"gbk")); 
//		while ((strs  = csvReader.readNext()) != null) {
//			System.out.println(Joiner.on(",").useForNull("").join(strs));
//		}
		
		
		
	}

}
