package com.adpushup.e3.Database.Cache;

import com.couchbase.client.java.document.JsonDocument;
import java.util.concurrent.atomic.AtomicInteger;

class CachedDocument {

    private final String _id;
    private JsonDocument _jsonDoc;

    private long _expiry;
    private final AtomicInteger _frequency;
    private boolean _customData;

    CachedDocument(String id, JsonDocument jsonDoc, long ttl) {
        _id = id;
        _jsonDoc = jsonDoc;

        _expiry = System.currentTimeMillis() + ttl;
        _frequency = new AtomicInteger(1);
    }

    CachedDocument(String id, JsonDocument jsonDoc, long ttl, boolean customData) {
        _id = id;
        _jsonDoc = jsonDoc;

        _expiry = System.currentTimeMillis() + ttl;
        _frequency = new AtomicInteger(1);
        _customData = customData;
    }

    public boolean isExpired() {
        return _expiry < System.currentTimeMillis();
    }

    public String getId() {
        return _id;
    }

    public Boolean isCustomData() {
        return _customData;
    }

    public JsonDocument getJsonDocument() {
        _frequency.incrementAndGet();
        return _jsonDoc;
    }

    public int getFrequency() {
        return _frequency.get();
    }

    public void updateJsonDocument(JsonDocument jsonDoc, long ttl) {
        _jsonDoc = jsonDoc;
        _expiry = System.currentTimeMillis() + ttl;
    }

    public void resetFrequency() {
        _frequency.set(0);
    }
}
