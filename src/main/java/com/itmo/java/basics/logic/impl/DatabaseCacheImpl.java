package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private final int MAX_CAPACITY = 5_000;

    Map<String, byte[]> cache = new LinkedHashMap<>(MAX_CAPACITY) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return size() > MAX_CAPACITY;
        }
    };

    @Override
    public byte[] get(String key) {
        if (key == null) {
            return null;
        }

        if (!cache.containsKey(key)) {
            return null;
        }

        return cache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        if (key == null) {
            return;
        }

        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        if (key == null) {
            return;
        }

        cache.remove(key);
    }
}
