package com.globalpayments.batch.exception;

public class ConnectionFailureException extends Exception {
	private static final long serialVersionUID = 2621527480788075239L;

	public ConnectionFailureException(String message) {
		super(message);
	}
}
