package com.generic.client.hdfs.exception;

public class CSVParsingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CSVParsingException(String exception) {
        super(exception);
    }


}