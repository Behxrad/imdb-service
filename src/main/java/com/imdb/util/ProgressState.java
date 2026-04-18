package com.imdb.util;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ProgressState {

    public final Map<String, AtomicInteger> state;

    public ProgressState(Set<String> items) {
        this.state = items.stream().collect(Collectors.toMap(x -> x, x -> new AtomicInteger(0)));
    }

    public void setProgress(String key, int progress) {
        state.computeIfAbsent(key, k -> new AtomicInteger()).set(progress);
    }
}