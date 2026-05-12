package org.spark.service.exception;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@ControllerAdvice
public class GlobalExceptionHandler  {
    private static Logger logger = Logger.getLogger(GlobalExceptionHandler.class.getName());

    //Custom exception handler for WorkflowException
    @ExceptionHandler(value= RESTSQLWorkflowException.class)
    public ResponseEntity<ErrorResponse> handleWorkflowException(RESTSQLWorkflowException ex){
        logger.log(Level.SEVERE, HttpStatus.INTERNAL_SERVER_ERROR + " :: " + ex.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(),
                ex.getMessage()));
    }

    //Check validations if you add validation rules on DTO class
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValidException(MethodArgumentNotValidException ex) {
        List<String> errorMessages = ex.getBindingResult().getFieldErrors()
                .stream().map(FieldError::getDefaultMessage).collect(Collectors.toList());
        logger.log(Level.SEVERE, HttpStatus.BAD_REQUEST + ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(),
                errorMessages.toString()));
    }


    //Check validations if you add validation rules on entity class
    @ExceptionHandler(value=ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolationException(ConstraintViolationException ex){
        logger.log(Level.SEVERE, HttpStatus.BAD_REQUEST + ex.getMessage());
        List<String> errorMessages = new ArrayList<>();
        for (ConstraintViolation<?> violation : ex.getConstraintViolations()) {
            errorMessages.add(violation.getMessage());
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(),
                errorMessages.toString()));
    }

    //When given wrong request param
    @ExceptionHandler(value= MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentTypeMismatchException(MethodArgumentTypeMismatchException ex){
        logger.log(Level.SEVERE, HttpStatus.METHOD_NOT_ALLOWED + ex.getMessage());
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(new ErrorResponse(HttpStatus.METHOD_NOT_ALLOWED.value(),
                ex.getMessage()));
    }

    //When given invalid request method
    @ExceptionHandler(value= HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleRequestMethodNotSupportedException(HttpRequestMethodNotSupportedException ex){
        logger.log(Level.SEVERE, HttpStatus.METHOD_NOT_ALLOWED + ex.getMessage());
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(new ErrorResponse(HttpStatus.METHOD_NOT_ALLOWED.value(),
                ex.getMessage()));
    }

    //
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(IllegalArgumentException ex) {
        logger.log(Level.SEVERE, HttpStatus.BAD_REQUEST+ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(),
                ex.getMessage()));
    }

    //When given invalid values to request body
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadableException(HttpMessageNotReadableException ex) {
        logger.log(Level.SEVERE, HttpStatus.BAD_REQUEST+ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(),
                ex.getMessage()));
    }

    //
    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoHandlerFoundException(NoHandlerFoundException ex) {
        logger.log(Level.SEVERE, HttpStatus.NOT_FOUND + ex.getRequestURL());
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse(HttpStatus.NOT_FOUND.value(), ex.getRequestURL()));
    }

    //Invalid media types
    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleUnsupportedMediaTypeException(HttpMediaTypeNotSupportedException ex) {
        logger.log(Level.SEVERE, String.valueOf(HttpStatus.UNSUPPORTED_MEDIA_TYPE), ex.getContentType());
        return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).body(new ErrorResponse(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value(), ex.getContentType().toString()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleCommonException(Exception ex){
        logger.log(Level.SEVERE, ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(),"OTHER: " +  ex.getMessage()));
    }
}

/*
 * Docs
 * https://medium.com/thefreshwrites/exception-handling-spring-boot-rest-api-c2656b575fee
 * https://github.com/amilairoshan/ErrorHandlingDemo/tree/master
 */
