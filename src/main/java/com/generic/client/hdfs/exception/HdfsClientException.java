package com.generic.client.hdfs.exception;

public class HdfsClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public HdfsClientException(String exception) {
        super(exception);
    }


}
