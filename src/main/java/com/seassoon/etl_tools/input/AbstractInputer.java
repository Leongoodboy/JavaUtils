package com.seassoon.etl_tools.input;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.seassoon.etl_tools.outout.OutPuter;

public abstract class AbstractInputer implements Inputer {

	protected String[] header =null;
	
	protected String[] lastHeader=null;
	

	protected final List<String> columns = new ArrayList<>();

	protected int rowNumber = 0;
	
	protected int recordNumer=0;

	// TODO Auto-generated method stub
	private String[] data = null;



	public String[] getCurrentData() {
		return data;
	}


	public int getRecordNumer() {
		return recordNumer;
	}


	@Override
	public void initialize() throws Exception {

		this.getHeader();
		
	}
	

	public abstract boolean read() throws Exception;


	@Override
	public void out(OutPuter outPuter) throws Exception {
		
	
		String[] temp=null;
		while ((temp = this.next()) != null) {
			data=temp;
			outPuter.process(data);
//			System.out.println(recordNumer);
		}
		if(this.getRecordNumer()==0 &&  header!= null){
			outPuter.process(null);
		}
			this.close();
			outPuter.close();
	
	
	}
	


}
