package com.tbank.bdp.sink.exception;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class ClickhouseException extends Exception {

    public ClickhouseException(String message) {
        super(message);
    }

    public ClickhouseException(String message, Throwable cause) {
        super(message, cause);
    }
}
