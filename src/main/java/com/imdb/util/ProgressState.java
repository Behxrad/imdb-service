package com.imdb.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ProgressState {

    public final Map<String, AtomicInteger> state;

    public ProgressState(Set<String> items) {
        this.state = new LinkedHashMap<>();
    }

    public void setProgress(String key, int progress) {
        state.computeIfAbsent(key, k -> new AtomicInteger()).set(progress);
    }
}