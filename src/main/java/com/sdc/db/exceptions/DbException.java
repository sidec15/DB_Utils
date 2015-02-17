/**
 * DbException.java
 */
package com.sdc.db.exceptions;

/**
 * @author Simone
 * 15/feb/2015
 */
public class DbException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public DbException() {
	}

	/**
	 * @param message
	 */
	public DbException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public DbException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public DbException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public DbException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
