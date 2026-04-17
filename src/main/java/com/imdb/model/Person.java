package com.imdb.model;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Builder
@Data
public class Person {

    private String id;
    private String primaryName;
    private Integer birthYear;
    private Integer deathYear;
    private Set<String> primaryProfessions;
    private Set<Title> knownForTitles;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Person person = (Person) o;
        return id != null && id.equals(person.id);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
