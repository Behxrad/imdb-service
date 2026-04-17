package com.imdb.mapper;

import com.imdb.dto.TitleDTO;
import com.imdb.model.Title;

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
                .build();
    }
}