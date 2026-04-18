package com.imdb.controller;

import com.imdb.dto.APIResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public APIResponse<?> handleGeneric(Exception ex) {
        return APIResponse.builder()
                .success(false)
                .error(ex.getMessage())
                .build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public APIResponse<?> handleBadRequest(IllegalArgumentException ex) {
        return APIResponse.builder()
                .success(false)
                .error(ex.getMessage())
                .build();
    }
}