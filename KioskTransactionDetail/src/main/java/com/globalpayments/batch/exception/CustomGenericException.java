package com.globalpayments.batch.exception;

public class CustomGenericException extends RuntimeException {

	private static final long serialVersionUID = -513118499829128767L;

	private final String message;
	
	public CustomGenericException(String msg){
		super();
		this.message = msg;
	}
	@Override
	public String toString(){
		return this.message;
	}
}
