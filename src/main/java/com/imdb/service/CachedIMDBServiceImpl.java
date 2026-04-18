package com.imdb.service;

import com.imdb.cache.CacheKey;
import com.imdb.cache.SimpleCache;
import com.imdb.dto.TitleDTO;
import com.imdb.dto.TitlesDTO;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class CachedIMDBServiceImpl implements IMDBService {

    private final IMDBService delegate;
    private final SimpleCache<CacheKey, Object> cache;

    public CachedIMDBServiceImpl(IMDBService delegate) {
        this.delegate = delegate;
        this.cache = new SimpleCache<>(Duration.ofDays(1), 200);
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
    public TitlesDTO titlesWithSameDirectorWriter(int page, int size) {
        return delegate.titlesWithSameDirectorWriter(page, size);
    }

    @Override
    public Set<TitleDTO> sharedTitles(String actor1, String actor2) {
        return cached(new CacheKey("sharedTitles", actor1, actor2),
                () -> delegate.sharedTitles(actor1, actor2));
    }

    @Override
    public Map<Integer, TitleDTO> genreYearlyRanking(String genre) {
        return cached(new CacheKey("genreYearlyRanking", genre),
                () -> delegate.genreYearlyRanking(genre));
    }
}
