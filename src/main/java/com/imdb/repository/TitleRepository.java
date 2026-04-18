package com.imdb.repository;

import com.imdb.model.Title;
import com.imdb.model.Titles;

import java.util.Map;
import java.util.Set;

public interface TitleRepository {

    Titles findTitlesSameDirectorWriterAlive(int page, int size);

    Set<Title> findCommonTitlesBetweenActors(String actorId1, String actorId2);

    Map<Integer, Title> findBestTitlesByYearForGenre(String genre);
}