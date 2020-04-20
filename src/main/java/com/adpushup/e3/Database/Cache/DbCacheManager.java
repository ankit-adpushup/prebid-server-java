package com.adpushup.e3.Database.Cache;

import com.adpushup.e3.Database.DbManager;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
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
            this.queryAndSetCustomData();
            cachedDoc = _cache.get(id);
        }
        return cachedDoc.getJsonDocument();
    }

    public void set(String id, JsonDocument jsonDoc) {
        CachedDocument cachedDoc = new CachedDocument(id, jsonDoc, DEFAULT_DOC_TTL, true);
        _cache.putIfAbsent(id, cachedDoc);
    }

    public ArrayList<JsonDocument> queryAndSetCustomData() {
        String query1 = "SELECT ownerEmail, siteId from `AppBucket` WHERE meta().id like 'site::%';";
        String query2 = "SELECT adServerSettings.dfp.prebidGranularityMultiplier FROM `AppBucket`"
                + " WHERE meta().id = 'user::%s';";
        String query3 = "SELECT RAW hbcf from `AppBucket` WHERE meta().id = 'hbdc::%s';";
        JsonObject jsonObj;
        JsonDocument jsonDoc;
        ArrayList<JsonDocument> docList = new ArrayList<JsonDocument>();
        for (N1qlQueryRow row : _bucket.query(N1qlQuery.simple(query1))) {
            jsonObj = row.value();
            String siteId = jsonObj.get("siteId").toString();
            for (N1qlQueryRow i : _bucket
                    .query(N1qlQuery.simple(String.format(query2, jsonObj.get("ownerEmail").toString())))) {
                jsonObj.put("prebidGranularityMultiplier", i.value().get("prebidGranularityMultiplier"));
            }
            for (N1qlQueryRow j : _bucket
                    .query(N1qlQuery.simple(String.format(query3, jsonObj.get("siteId").toString())))) {
                JsonObject hbcf = j.value();
                JsonObject revShareObj = JsonObject.create();
                Set<String> bidderNames = hbcf.getNames();
                for (String bidder : bidderNames) {
                    revShareObj.put(bidder, ((JsonObject) hbcf.get(bidder)).get("revenueShare"));
                }
                jsonObj.put("revenueShare", revShareObj);
            }
            jsonDoc = JsonDocument.create(jsonObj.get("siteId").toString(), jsonObj);
            docList.add(jsonDoc);
            set(siteId, jsonDoc);
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
                                    String query1 = "SELECT ownerEmail, siteId from `AppBucket` WHERE meta().id like 'site::%';";
                                    String query2 = "SELECT adServerSettings.dfp.prebidGranularityMultiplier FROM `AppBucket`"
                                            + " WHERE meta().id like 'user::%' AND email='%s';";
                                    String query3 = "SELECT RAW hbcf from `AppBucket` WHERE meta().id = 'hbdc::%s';";
                                    JsonObject jsonObj;
                                    JsonDocument jsonDoc;
                                    for (N1qlQueryRow row : _bucket.query(N1qlQuery.simple(query1))) {
                                        jsonObj = row.value();
                                        for (N1qlQueryRow row2 : _bucket.query(N1qlQuery
                                                .simple(String.format(query2, jsonObj.get("ownerEmail").toString())))) {
                                            jsonObj.put("prebidGranularityMultiplier",
                                                    row2.value().get("prebidGranularityMultiplier"));
                                        }
                                        for (N1qlQueryRow row3 : _bucket.query(N1qlQuery
                                                .simple(String.format(query3, jsonObj.get("siteId").toString())))) {
                                            JsonObject hbcf = row3.value();
                                            JsonObject revShareObj = JsonObject.create();
                                            Set<String> bidderNames = hbcf.getNames();
                                            for (String bidder : bidderNames) {
                                                revShareObj.put(bidder,
                                                        ((JsonObject) hbcf.get(bidder)).get("revenueShare"));
                                            }
                                            jsonObj.put("revenueShare", revShareObj);
                                        }
                                        jsonDoc = JsonDocument.create(jsonObj.get("siteId").toString(), jsonObj);
                                        fetchedData.add(jsonDoc);
                                    }
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
