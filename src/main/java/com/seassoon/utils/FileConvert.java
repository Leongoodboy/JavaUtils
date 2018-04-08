package com.seassoon.utils;

import com.seassoon.etl_tools.input.CsvInputer;
import com.seassoon.etl_tools.input.Inputer;
import com.seassoon.etl_tools.outout.OutPuter;

public class FileConvert {

	
	public static  void convert(Inputer inputer,OutPuter outPuter) throws Exception{
		
		inputer.out(outPuter);
		
		
	}
	
}
