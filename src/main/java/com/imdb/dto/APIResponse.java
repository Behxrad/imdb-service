package com.imdb.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class APIResponse<T> {
    private boolean success;
    private T data;
}