package com.imdb.repository.tsv;

import com.imdb.model.Title;
import com.imdb.repository.TitleRepository;
import com.imdb.repository.cache.CacheKey;
import com.imdb.repository.cache.SimpleCache;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class CachedTitleRepositoryImpl implements TitleRepository {

    private final TitleRepository delegate;
    private final SimpleCache<CacheKey, Object> cache;

    public CachedTitleRepositoryImpl(TitleRepository delegate) {
        this.delegate = delegate;
        this.cache = new SimpleCache<>(Duration.ofMinutes(10), 200);
    }

    @SuppressWarnings("unchecked")
    private <T> T cached(CacheKey key, Supplier<T> supplier) {
        Object cached = cache.get(key);
        if (cached != null) return (T) cached;

        T value = supplier.get();
        cache.put(key, value);
        return value;
    }

    @Override
    public Set<Title> findTitlesSameDirectorWriterAlive() {
        return cached(new CacheKey("sameDirectorWriter"), delegate::findTitlesSameDirectorWriterAlive);
    }

    @Override
    public Set<Title> findCommonTitlesBetweenActors(String actor1, String actor2) {
        return cached(new CacheKey("commonTitles", actor1, actor2),
                () -> delegate.findCommonTitlesBetweenActors(actor1, actor2));
    }

    @Override
    public Map<Integer, Title> findBestTitlesByYearForGenre(String genre) {
        return cached(new CacheKey("genreRanking", genre),
                () -> delegate.findBestTitlesByYearForGenre(genre));
    }
}