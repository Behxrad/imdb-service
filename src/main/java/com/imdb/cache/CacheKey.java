package com.imdb.cache;

import java.util.Arrays;
import java.util.Objects;

public class CacheKey {

    private final String method;
    private final Object[] params;

    public CacheKey(String method, Object... params) {
        this.method = method;
        this.params = params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CacheKey)) return false;
        CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(method, cacheKey.method) &&
                Arrays.deepEquals(params, cacheKey.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, Arrays.deepHashCode(params));
    }
}