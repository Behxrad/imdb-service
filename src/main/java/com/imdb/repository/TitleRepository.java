package com.imdb.repository;

import com.imdb.model.Title;

import java.util.Map;
import java.util.Set;

public interface TitleRepository {

    Set<Title> findTitlesSameDirectorWriterAlive();

    Set<Title> findCommonTitlesBetweenActors(String actorId1, String actorId2);

    Map<Integer, Title> findBestTitlesByYearForGenre(String genre);
}