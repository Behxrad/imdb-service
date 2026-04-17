package com.imdb.repository.cache;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleCache<K, V> {

    private static class Entry<V> {
        V value;
        long timestamp;
        long lastAccess;

        Entry(V value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
            this.lastAccess = this.timestamp;
        }
    }

    private final long ttlMillis;
    private final int maxSize;
    private final Map<K, Entry<V>> map = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public SimpleCache(Duration ttl, int maxSize) {
        this.ttlMillis = ttl.toMillis();
        this.maxSize = maxSize;
    }

    public V get(K key) {
        Entry<V> entry = map.get(key);
        if (entry == null) return null;

        long now = System.currentTimeMillis();

        if (now - entry.timestamp > ttlMillis) {
            map.remove(key);
            return null;
        }

        entry.lastAccess = now;
        return entry.value;
    }

    public void put(K key, V value) {
        lock.lock();
        try {
            if (map.size() >= maxSize) {
                evict();
            }
            map.put(key, new Entry<>(value));
        } finally {
            lock.unlock();
        }
    }

    private void evict() {
        K oldestKey = null;
        long oldest = Long.MAX_VALUE;

        for (Map.Entry<K, Entry<V>> e : map.entrySet()) {
            long score = Math.min(e.getValue().timestamp, e.getValue().lastAccess);
            if (score < oldest) {
                oldest = score;
                oldestKey = e.getKey();
            }
        }

        if (oldestKey != null) {
            map.remove(oldestKey);
        }
    }
}