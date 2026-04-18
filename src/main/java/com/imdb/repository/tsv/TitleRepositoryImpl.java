package com.imdb.repository.tsv;

import com.imdb.cache.SimpleCache;
import com.imdb.model.Title;
import com.imdb.model.Titles;
import com.imdb.repository.TitleRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Year;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Repository
public class TitleRepositoryImpl implements TitleRepository {

    private final TSVDataStore store;
    private final SimpleCache<String, List<Title>> cache;

    public TitleRepositoryImpl(TSVDataStore store) {
        this.store = store;
        this.cache = new SimpleCache<>(Duration.ofDays(1), 200);
    }

    public Titles findTitlesSameDirectorWriterAlive(int page, int size) {
        List<Title> all = cache.get("TitlesSameDirectorWriterAlive");
        page--;

        int from = Math.min(page * size, all.size());
        int to = Math.min(from + size, all.size());

        return Titles.builder()
                .titles(all.subList(from, to))
                .page(page)
                .size(size)
                .totalSize(all.size())
                .build();
    }

    @Scheduled(fixedRate = 5 * 60 * 60 * 1000)
    public void findTitlesSameDirectorWriterAlive() {
        Queue<Title> result = new ConcurrentLinkedQueue<>();

        int threadPoolSize = Math.min(8, Runtime.getRuntime().availableProcessors() / 2);

        try (ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize)) {
            for (long i = 1; i < store.getMaxTitleID(); i++) {
                long titleID = i;

                executor.submit(() -> {
                    Map<String, Set<Title.Principal>> principals = store.readTitlePrincipals(titleID);

                    Set<String> directors = principals.getOrDefault("director", new HashSet<>())
                            .stream()
                            .filter(x -> Objects.nonNull(x.getPerson()))
                            .map(x -> x.getPerson().getId()).collect(Collectors.toSet());
                    Set<String> writers = principals.getOrDefault("writer", new HashSet<>())
                            .stream()
                            .filter(x -> Objects.nonNull(x.getPerson()))
                            .map(x -> x.getPerson().getId()).collect(Collectors.toSet());

                    if (directors.isEmpty() || writers.isEmpty()) return;

                    for (String directorID : directors) {
                        if (writers.contains(directorID) && store.isPersonAlive(TSVIndexer.parseId(directorID))) {
                            Title title = store.readTitleBasics(titleID);
                            title.setPrincipals(principals);
                            title.setRating(store.readTitleRating(titleID));

                            result.add(title);
                            cache.put("TitlesSameDirectorWriterAlive", new ArrayList<>(result));
                        }
                    }
                });
            }
        }
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
        ConcurrentHashMap<Integer, Title> bestTitlesByYear = new ConcurrentHashMap<>();
        int currentYear = Year.now().getValue();

        int threadPoolSize = Math.min(8, Runtime.getRuntime().availableProcessors() / 2);

        try (ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize)) {
            for (int year = 1800; year <= currentYear; year++) {
                final int y = year;

                executor.submit(() -> {
                    Set<Title> titles = store.readGenreTilesYearly(genre, y);
                    if (titles == null || titles.isEmpty()) {
                        return;
                    }

                    titles.stream()
                            .filter(t -> t.getRating() != null)
                            .max(Comparator.comparing((Title t) -> t.getRating().getAverageRating())
                                    .thenComparing(t -> t.getRating().getNumberOfVotes())).ifPresent(best -> bestTitlesByYear.put(y, best));

                });
            }
        }

        return bestTitlesByYear;
    }
}