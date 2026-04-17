package com.imdb.repository.tsv;

import com.imdb.model.Title;
import com.imdb.repository.TitleRepository;

import java.time.Year;
import java.util.*;
import java.util.stream.Collectors;

public class TitleRepositoryImpl implements TitleRepository {

    private final TSVDataStore store;

    public TitleRepositoryImpl(TSVDataStore store) {
        this.store = store;
    }

    public Set<Title> findTitlesSameDirectorWriterAlive() {
        Set<Title> result = new HashSet<>();
        int limit = 10;

        for (long i = 1; i < store.getMaxTitleID(); i++) {
            Map<String, Set<Title.Principal>> principals = store.readTitlePrincipals(i);

            Set<String> directors = principals.getOrDefault("director", new HashSet<>())
                    .stream()
                    .filter(x -> Objects.nonNull(x.getPerson()))
                    .map(x -> x.getPerson().getId()).collect(Collectors.toSet());
            Set<String> writers = principals.getOrDefault("writer", new HashSet<>())
                    .stream()
                    .filter(x -> Objects.nonNull(x.getPerson()))
                    .map(x -> x.getPerson().getId()).collect(Collectors.toSet());

            if (directors.isEmpty() || writers.isEmpty()) continue;

            for (String directorID : directors) {
                if (writers.contains(directorID) && store.isPersonAlive(TSVIndexer.parseId(directorID))) {
                    Title title = store.readTitleBasics(i);
                    title.setPrincipals(principals);
                    title.setRating(store.readTitleRating(i));

                    result.add(title);
                }
            }

            if (result.size() == limit) break;
        }

        return result;
    }

    public Set<Title> findCommonTitlesBetweenActors(String actorId1, String actorId2) {
        long id1 = store.getPersonIDByTheirName(actorId1);
        long id2 = store.getPersonIDByTheirName(actorId2);

        Set<Title> titles1 = store.readTitlesByActorID(id1);
        Set<Title> titles2 = store.readTitlesByActorID(id2);

        if (titles1.isEmpty() || titles2.isEmpty()) {
            return new HashSet<>();
        }

        titles1.retainAll(titles2);

        return titles1;
    }

    public Map<Integer, Title> findBestTitlesByYearForGenre(String genre) {
        Map<Integer, Title> bestTitlesByYear = new HashMap<>();

        for (int year = 1800; year <= Year.now().getValue(); year++) {
            Set<Title> titles = store.readGenreTilesYearly(genre, year);
            if (titles == null || titles.isEmpty()) continue;

            Title best = titles.stream()
                    .filter(t -> t.getRating() != null)
                    .max((t1, t2) -> {
                        int cmp = Float.compare(t1.getRating().getAverageRating(), t2.getRating().getAverageRating());
                        if (cmp == 0) {
                            cmp = Integer.compare(t1.getRating().getNumberOfVotes(), t2.getRating().getNumberOfVotes());
                        }
                        return cmp;
                    })
                    .orElse(null);

            if (best != null) {
                bestTitlesByYear.put(year, best);
            }
        }

        return bestTitlesByYear;
    }
}