package com.imdb.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TitlesDTO {

    private List<TitleDTO> titles;
    private int page;
    private int size;
    private int totalSize;
}
