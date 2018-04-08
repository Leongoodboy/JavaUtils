package com.seassoon.etl_tools.outout;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.seassoon.etl_tools.outout.OutPuter;

public abstract class AbstractOutPuter implements OutPuter {

	protected String[] header =null;

	protected BlockingQueue<String[]> queue;
	
	protected String encoding="UTF-8";
	

	public AtomicInteger getProcessNum() {
		return processNum;
	}



	protected AtomicInteger processNum=new AtomicInteger(0);
	
	protected  OutputStream outputStream ;
	
	
	
	public OutputStream getOutputStream() {
		return outputStream;
	}
	
	

	
	
}
