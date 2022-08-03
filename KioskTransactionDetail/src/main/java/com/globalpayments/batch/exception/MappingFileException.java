package com.globalpayments.batch.exception;

public class MappingFileException extends IllegalArgumentException {

	/**
	 * Exception specifically for mapping file related errors
	 */
	private static final long serialVersionUID = 4016517154419965525L;
	
	private final String message;
	
	public MappingFileException(String msg){
		super();
		this.message = msg;
	}
	@Override
	public String toString(){
		return this.message;
	}
}
