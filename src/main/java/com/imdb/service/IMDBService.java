package com.imdb.service;

import com.imdb.dto.TitleDTO;
import com.imdb.dto.TitlesDTO;

import java.util.Map;
import java.util.Set;

public interface IMDBService {

    TitlesDTO titlesWithSameDirectorWriter(int page, int size);

    Set<TitleDTO> sharedTitles(String actor1, String actor2);

    Map<Integer, TitleDTO> genreYearlyRanking(String genre);
}
