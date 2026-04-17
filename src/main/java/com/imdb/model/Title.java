package com.imdb.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Builder
@Data
public class Title {

    private String id;
    private String type;
    private String primaryTitle;
    private String originalTitle;
    private boolean isAdult;
    private Integer startYear;
    private Integer endYear;
    private Integer runtimeMinutes;
    private Set<String> genres;

    private Map<String, Set<Principal>> principals;
    private Rating rating;

    @Builder
    @Data
    public static class Principal {

        private int ordering;
        private String category;
        private String job;
        private Set<String> characters;
        private Person person;
    }

    @Builder
    @Data
    public static class Rating {
        private float averageRating;
        private int numberOfVotes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Title title = (Title) o;
        return id != null && id.equals(title.id);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}