package com.generic.client.hdfs.controller;

import com.generic.client.hdfs.exception.CSVParsingException;
import com.generic.client.hdfs.exception.HdfsClientException;
import com.generic.client.hdfs.models.ErrorDetails;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.WebRequest;

import java.io.IOException;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@ControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private String exceptionMsg = "Request failure due to Exception {} : {} occured.";

    @ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "IOException occured")
    @ExceptionHandler(IOException.class)
    public void handleIOException() {
        LOGGER.error("IOException handler executed");
    }

    @ExceptionHandler({CSVParsingException.class})
    public final ResponseEntity<ErrorDetails> handleHbaseClientExceptionException(CSVParsingException ex, WebRequest request) {
        LOGGER.error(exceptionMsg, ex.getClass(), ex.getMessage());
        ErrorDetails errorDetails = new ErrorDetails(ex.getMessage(),
                request.getDescription(false));
        return new ResponseEntity<>(errorDetails, INTERNAL_SERVER_ERROR);
    }
    @ExceptionHandler({HdfsClientException.class})
    public final ResponseEntity<ErrorDetails> handleHbaseClientExceptionException(HdfsClientException ex, WebRequest request) {
        LOGGER.error(exceptionMsg, ex.getClass(), ex.getMessage());
        ErrorDetails errorDetails = new ErrorDetails(ex.getMessage(),
                request.getDescription(false));
        return new ResponseEntity<>(errorDetails, INTERNAL_SERVER_ERROR);
    }

}
