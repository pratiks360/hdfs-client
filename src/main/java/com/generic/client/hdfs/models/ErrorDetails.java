package com.generic.client.hdfs.models;


public class ErrorDetails {

    private String message;
    private String code;

    public String getMessage() {
        return message;
    }

    public String getCode() {
        return code;
    }

    public ErrorDetails() {
    }

    public ErrorDetails(String message, String code) {
        super();
        this.message = message;
        this.code = code;
    }
}

