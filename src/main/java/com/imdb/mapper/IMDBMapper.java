package com.imdb.mapper;

import com.imdb.dto.TitleDTO;
import com.imdb.model.Title;

import java.util.stream.Collectors;

public class IMDBMapper {

    public static TitleDTO toDTO(Title t) {
        if (t == null) return null;

        return TitleDTO.builder()
                .id(t.getId())
                .primaryTitle(t.getPrimaryTitle())
                .originalTitle(t.getOriginalTitle())
                .startYear(t.getStartYear())
                .endYear(t.getEndYear())
                .genres(t.getGenres())
                .averageRating(t.getRating() != null ? t.getRating().getAverageRating() : null)
                .numberOfVotes(t.getRating() != null ? t.getRating().getNumberOfVotes() : null)
                .writers(t.getPrincipals().entrySet().stream()
                        .filter(x -> x.getKey().equals("writer"))
                        .flatMap(x -> x.getValue().stream())
                        .map(x -> x.getPerson().getPrimaryName())
                        .collect(Collectors.toList()))
                .directors(t.getPrincipals().entrySet().stream()
                        .filter(x -> x.getKey().equals("director"))
                        .flatMap(x -> x.getValue().stream())
                        .map(x -> x.getPerson().getPrimaryName())
                        .collect(Collectors.toList()))
                .build();
    }
}