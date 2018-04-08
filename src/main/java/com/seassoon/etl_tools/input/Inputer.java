package com.seassoon.etl_tools.input;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import com.seassoon.etl_tools.outout.OutPuter;

public interface Inputer {
	
	
	/**
	 * 初始化
	 * @throws Exception
	 */
	 public void initialize() throws Exception ;

	 /**
	  * 获取头
	  * @return
	 * @throws Exception 
	  */
	 public String[] getHeader() throws Exception;
	 
	 /**
	  * 下一行
	  * @return
	  */
	 public String[] next() throws Exception;	 
	 
	 /**
	  * 关闭
	  * @return
	  */
	 public  void close() throws Exception;  
	 
	 /**
	  * 输出
	  * @param outPuter
	  * @throws Exception
	  */
	 public void out(OutPuter outPuter) throws Exception;
	 
	 

}
