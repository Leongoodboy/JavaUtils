package com.seassoon.etl_tools.outout;

import java.io.IOException;

import com.seassoon.etl_tools.input.Inputer;

public interface OutPuter {

	
	
	public void initialize() throws  Exception;

	public void process(String[] data) throws IOException, Exception ;
	
	
	public void close();
	
	
	
}
