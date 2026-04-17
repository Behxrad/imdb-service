package com.imdb.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TitleDTO {
    private String id;
    private String primaryTitle;
    private String originalTitle;
    private Integer startYear;
    private Integer endYear;
    private Set<String> genres;
    private Float averageRating;
    private Integer numberOfVotes;
}