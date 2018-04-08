package com.seassoon.etl_tools.outout;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FileOutPuter  extends AbstractOutPuter {

	/**
	 * 是否自动输出列头
	 */
	private boolean autoProcessHeader=true;


	@Override
	public void initialize() throws Exception {
		
		
		if(header != null){
			process(header);	
		}
	
		
		
	}


	/**
	 * 文件路径
	 */
	protected String path;
	
	
	public FileOutPuter(OutputStream outputStream,String[] header) {
		this.outputStream = outputStream;
		this.header=header;
	}
	
	public  FileOutPuter(String path,String[] header) throws IOException { 

		this.path = path;

		this.outputStream = new FileOutputStream(new File(path));
		this.header=header;

	}
	
	
	
	public void setPath(String path) {
		this.path = path;
	}


	/**
	 * 输出流
	 */
	protected  OutputStream outputStream ;
	
	
	
	public OutputStream getOutputStream() {
		return outputStream;
	}



	public void setOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
	}



	@Override
	public void close() {
		try {
			outputStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}




}
