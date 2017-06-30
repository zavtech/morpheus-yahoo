/**
 * Copyright 2014, Zavtech.com
 */
package com.zavtech.finance.yahoo;

/**
 * Exception description here...
 *
 * @author Xavier Witdouck
 */
public class YahooException extends RuntimeException {

    /**
     * Constructor
     *
     * @param message the exception message
     */
    public YahooException(String message) {
        this(message, null);
    }

    /**
     * Constructor
     *
     * @param message the exception message
     * @param cause   the root cause, null permitted
     */
    public YahooException(String message, Throwable cause) {
        super(message, cause);
    }
}
