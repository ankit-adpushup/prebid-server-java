package com.adpushup.e3.Database.Cache;

import com.adpushup.e3.Database.DbManager;
import com.adpushup.e3.Database.Callback;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DbCacheManager {

    private final Long DEFAULT_DOC_TTL = 300000l;

    private final int _maxCacheSize;
    private final int _refreshInterval;
    private final Bucket _bucket;
    private final DbManager _db;

    private final ConcurrentHashMap<String, CachedDocument> _cache;

    private final Timer _cacheMaintenanceTimer = new Timer();
    private CacheMaintenanceTask _currentTask;

    // Used in stats
    private int _averageCacheSize = 0;
    private int _sampleCount = 0;
    private int _peakCacheSize = 0;

    public DbCacheManager(int maxCacheSize, int refreshInterval, Bucket bucket, DbManager db) {
        _maxCacheSize = maxCacheSize;
        _refreshInterval = refreshInterval;
        _bucket = bucket;
        _db = db;

        _cache = new ConcurrentHashMap<>(_maxCacheSize);

        scheduleTask(_refreshInterval);
    }

    public void flush() {
        _cache.clear();
    }

    public JsonDocument get(String id) {
        CachedDocument cachedDoc = _cache.get(id);
        if (cachedDoc == null) {
            JsonDocument jsonDoc = _bucket.get(id, 30, TimeUnit.SECONDS);

            Long ttl = null;

            if (jsonDoc != null) {
                ttl = jsonDoc.content().getLong("cacheTtl");
            }

            if (ttl == null) {
                ttl = DEFAULT_DOC_TTL;
            }

            if (ttl < 1) {
                // ttl=0 means dont cache the document
                return jsonDoc;
            }

            cachedDoc = new CachedDocument(id, jsonDoc, ttl);
            _cache.putIfAbsent(id, cachedDoc);
        }

        return cachedDoc.getJsonDocument();
    }

    public JsonDocument getCustom(String id) {
        CachedDocument cachedDoc = _cache.get(id);
        if (cachedDoc == null) {
            return null; // TODO handle what happens if cachedDoc is null
        }
        return cachedDoc.getJsonDocument();
    }

    public void set(String id, JsonDocument jsonDoc, Callback func) {
        CachedDocument cachedDoc = new CachedDocument(id, jsonDoc, DEFAULT_DOC_TTL, true, func);
        _cache.putIfAbsent(id, cachedDoc);
    }

    public ArrayList<JsonDocument> queryAndSetCustomData(Callback func) {
        ArrayList<JsonDocument> docList = func.call(_bucket);
        for (JsonDocument doc: docList) {
            set(doc.content().get("siteId").toString(), doc, func);
        }
        return docList;
    }

    public int getAverageSize() {
        return _averageCacheSize;
    }

    public int getCurrentSize() {
        return _cache.size();
    }

    public Integer getPeakSize() {
        return _peakCacheSize;
    }

    public int getMaxSize() {
        return _maxCacheSize;
    }

    public int getSampleCount() {
        return _sampleCount;
    }

    public void close() {
        flush();

        if (_currentTask != null) {
            _cacheMaintenanceTimer.cancel();
            _currentTask = null;
        }
    }

    private void scheduleTask(int refreshInterval) {
        _currentTask = new CacheMaintenanceTask();
        _cacheMaintenanceTimer.schedule(_currentTask, refreshInterval);
    }

    private class CacheMaintenanceTask extends TimerTask {

        @Override
        public void run() {
            try {
                Boolean customDataFetched = false;
                ArrayList<JsonDocument> fetchedData = new ArrayList<JsonDocument>();
                // load all cached items to temp list
                List<CachedDocument> cachedItems = new ArrayList<>(_cache.values());

                int cacheSize = cachedItems.size();

                _sampleCount++;
                double n = _sampleCount;
                _averageCacheSize += (int) Math.ceil((cacheSize - _averageCacheSize) / n);

                if (cacheSize > _peakCacheSize) {
                    _peakCacheSize = cacheSize;
                }

                if (cacheSize > _maxCacheSize) {
                    // sort temp list by used frequency in descending order
                    Collections.sort(cachedItems, new Comparator<CachedDocument>() {
                        @Override
                        public int compare(CachedDocument doc1, CachedDocument doc2) {
                            return doc2.getFrequency() - doc1.getFrequency();
                        }
                    });

                    // remove least frequently used items from cache
                    for (int i = _maxCacheSize; i < cacheSize; i++) {
                        _cache.remove(cachedItems.get(i).getId());
                    }

                    cacheSize = _maxCacheSize;
                }

                // update expired cache items
                for (int i = 0; i < cacheSize; i++) {
                    CachedDocument cachedItem = cachedItems.get(i);

                    if (cachedItem.isExpired()) {
                        try {
                            if (cachedItem.getFrequency() == 0) {
                                _cache.remove(cachedItem.getId());
                                continue;
                            }

                            if (!cachedItem.isCustomData()) {
                                JsonDocument jsonDoc = _bucket.get(cachedItem.getId(), 10, TimeUnit.SECONDS);
                                Long ttl = null;

                                if (jsonDoc != null) {
                                    ttl = jsonDoc.content().getLong("cacheTtl");
                                }

                                if (ttl == null) {
                                    ttl = DEFAULT_DOC_TTL;
                                }

                                if (ttl < 1) {
                                    // ttl=0 means remove the cached document
                                    _cache.remove(cachedItem.getId());
                                } else {
                                    cachedItem.updateJsonDocument(jsonDoc, ttl);
                                }
                            } else {
                                if (!customDataFetched) {
                                    fetchedData = cachedItem.func.call(_bucket);
                                }
                                for (JsonDocument doc : fetchedData) {
                                    if (doc.id() == cachedItem.getId()) {
                                        cachedItem.updateJsonDocument(doc, DEFAULT_DOC_TTL);
                                    }
                                }
                            }
                        } catch (Exception ex) {
                            DbManager.insertSystemLog(_db, "DbCacheManager.CacheMaintenanceTask", ex);
                        }
                    }

                    cachedItem.resetFrequency();
                }
            } finally {
                scheduleTask(_refreshInterval);
            }
        }
    }
}
