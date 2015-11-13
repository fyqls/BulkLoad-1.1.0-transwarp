package com.transwarp.hbase.bulkload;

public class FormatException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FormatException(Exception e) {
		super(e);
	}
	public FormatException(String msg){
		super(msg);
	}

}
