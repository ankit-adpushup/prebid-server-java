package com.adpushup.e3.Database;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.eaio.uuid.UUID;
import java.io.Closeable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.functions.Func1;

public class DbManager implements Closeable {

    // variables
    final Cluster _cluster;

    Bucket _apAppBucket;
    Bucket _apGlobalBucket;
    Bucket _apStatsBucket;
    Bucket _apLocalBucket;
    Bucket _apCreativeQaBucket;
    Bucket _newAppBucket;

    final static String _apRegion;
    final static String _localHostname;

    final static int _systemLogExpirySeconds = 30 * 24 * 60 * 60;

    static {
        _apRegion = System.getenv("AP_REGION");

        String localHostname;

        try {
            localHostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            localHostname = null;
        }

        if ((_apRegion != null) && (localHostname != null) && !localHostname.startsWith(_apRegion)) {
            localHostname = _apRegion + "-" + localHostname;
        }

        _localHostname = localHostname;
    }

    // constructor
    public DbManager(String[] couchbaseIPs, String username, String password) {
        _cluster = CouchbaseCluster.create(couchbaseIPs);
        _cluster.authenticate(username, password);
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            _cluster.disconnect();
        } finally {
            super.finalize();
        }
    }

    // public
    public Bucket getAppBucket() {
        if ((_apAppBucket == null) || _apAppBucket.isClosed()) {
            synchronized (_cluster) {
                if ((_apAppBucket == null) || _apAppBucket.isClosed()) {
                    _apAppBucket = _cluster.openBucket("apAppBucket", 30, TimeUnit.SECONDS);
                }
            }
        }

        return _apAppBucket;
    }

    public Bucket getGlobalBucket() {
        if ((_apGlobalBucket == null) || _apGlobalBucket.isClosed()) {
            synchronized (_cluster) {
                if ((_apGlobalBucket == null) || _apGlobalBucket.isClosed()) {
                    _apGlobalBucket = _cluster.openBucket("apGlobalBucket", 30, TimeUnit.SECONDS);
                }
            }
        }

        return _apGlobalBucket;
    }

    public Bucket getStatsBucket() {
        if ((_apStatsBucket == null) || _apStatsBucket.isClosed()) {
            synchronized (_cluster) {
                if ((_apStatsBucket == null) || _apStatsBucket.isClosed()) {
                    _apStatsBucket = _cluster.openBucket("apStatsBucket", 30, TimeUnit.SECONDS);
                }
            }
        }

        return _apStatsBucket;
    }

    public Bucket getLocalBucket() {
        if ((_apLocalBucket == null) || _apLocalBucket.isClosed()) {
            synchronized (_cluster) {
                if ((_apLocalBucket == null) || _apLocalBucket.isClosed()) {
                    _apLocalBucket = _cluster.openBucket("apLocalBucket", 30, TimeUnit.SECONDS);
                }
            }
        }

        return _apLocalBucket;
    }

    public Bucket getCreativeQaBucket() {
        if ((_apCreativeQaBucket == null) || _apCreativeQaBucket.isClosed()) {
            synchronized (_cluster) {
                if ((_apCreativeQaBucket == null) || _apCreativeQaBucket.isClosed()) {
                    _apCreativeQaBucket = _cluster.openBucket("CreativeQA", 30, TimeUnit.SECONDS);
                }
            }
        }

        return _apCreativeQaBucket;
    }

    public Bucket getNewAppBucket() {
        if ((_newAppBucket == null) || _newAppBucket.isClosed()) {
            synchronized (_cluster) {
                if ((_newAppBucket == null) || _newAppBucket.isClosed()) {
                    _newAppBucket = _cluster.openBucket("AppBucket", 30, TimeUnit.SECONDS);
                }
            }
        }

        return _newAppBucket;
    }

    @Override
    public void close() {
        _cluster.disconnect();
    }

    public String getApRegion() {
        return _apRegion;
    }

    public String getLocalHostName() {
        return _localHostname;
    }

    // static
    public static void insertSystemLog(DbManager db, int type, String source, String message, String details) {
        insertSystemLog(db, type, source, message, details, null);
    }

    public static void insertSystemLog(DbManager db, int type, String source, String message, String details,
            String debugData) {
        String logKey = "slog::" + (new UUID()).toString();
        JsonObject json = JsonObject.create();

        json.put("type", type);
        json.put("date", System.currentTimeMillis());
        json.put("source", source);
        json.put("message", message);
        json.put("details", details);
        json.put("debugData", debugData);
        json.put("hostname", _localHostname);
        json.put("region", _apRegion);

        try {
            db.getGlobalBucket().insert(JsonDocument.create(logKey, _systemLogExpirySeconds, json), 30,
                    TimeUnit.SECONDS);
        } catch (Exception ex) {
        }
    }

    public static void insertSystemLog(DbManager db, String source, Exception ex) {
        insertSystemLog(db, source, ex, null);
    }

    public static void insertSystemLog(DbManager db, String source, Exception ex, String debugData) {
        insertSystemLog(db, 3, source, ex.toString(), getStackTraceFromException(ex), debugData);
    }

    public static String getStackTraceFromException(Exception ex) {
        StringBuilder output = new StringBuilder(1024);

        for (StackTraceElement stackTrace : ex.getStackTrace()) {
            output.append(stackTrace.toString());
            output.append("\r\n");
        }

        return output.toString();
    }

    public static <T> JsonObject findJsonObject(JsonArray jsonArray, String keyName, T matchValue) {
        for (Object obj : jsonArray) {
            JsonObject jsonObj = (JsonObject) obj;
            Object objKey = jsonObj.get(keyName);

            if ((objKey != null) && objKey.equals(matchValue)) {
                return jsonObj;
            }
        }

        return null;
    }

    public static List<JsonDocument> bulkRead(final Bucket bucket, List<String> docIds, long timeout, int retries,
            final boolean onErrorResumeNext) {
        try {
            List<JsonDocument> docList = Observable.from(docIds).flatMap(new Func1<String, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(String docId) {
                    return bucket.async().get(docId);
                }
            }).retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, timeout)).max(retries).build())
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
                        @Override
                        public Observable<? extends JsonDocument> call(Throwable throwable) {
                            if (onErrorResumeNext) {
                                return Observable.empty();
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }).toList().toBlocking().single();

            return docList;
        } catch (NoSuchElementException ex) {
            // no items available to read
            return new ArrayList<>();
        }
    }

    public static void bulkWrite(final Bucket bucket, List<JsonDocument> docList, long timeout, int retries) {
        if (docList.isEmpty()) {
            return;
        }

        try {
            Observable.from(docList).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(JsonDocument doc) {
                    return bucket.async().upsert(doc);
                }
            }).retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, timeout)).max(retries).build()).toBlocking().last();
        } catch (NoSuchElementException ex) {
            // no item returned
        }
    }

    public static void bulkDelete(final Bucket bucket, List<String> docIds, long timeout, int retries) {
        if (docIds.isEmpty()) {
            return;
        }

        try {
            Observable.from(docIds).flatMap(new Func1<String, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(String docId) {
                    return bucket.async().remove(docId);
                }
            }).retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, timeout)).max(retries).build())
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
                        @Override
                        public Observable<? extends JsonDocument> call(Throwable throwable) {
                            if (throwable instanceof DocumentDoesNotExistException) {
                                return Observable.empty();
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }).toBlocking().last();
        } catch (NoSuchElementException ex) {
            // no items returned
        }
    }

    public static void bulkDeleteDocs(final Bucket bucket, List<JsonDocument> docList, long timeout, int retries) {
        if (docList.isEmpty()) {
            return;
        }

        try {
            Observable.from(docList).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(JsonDocument doc) {
                    return bucket.async().remove(doc.id());
                }
            }).retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, timeout)).max(retries).build())
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
                        @Override
                        public Observable<? extends JsonDocument> call(Throwable throwable) {
                            if (throwable instanceof DocumentDoesNotExistException) {
                                return Observable.empty();
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }).toBlocking().last();
        } catch (NoSuchElementException ex) {
            // no items returned
        }
    }

    public static void bulkTouch(final Bucket bucket, List<String> docIds, final int expiry, long timeout,
            int retries) {
        if (docIds.isEmpty()) {
            return;
        }

        try {
            Observable.from(docIds).flatMap(new Func1<String, Observable<Boolean>>() {
                @Override
                public Observable<Boolean> call(String docId) {
                    return bucket.async().touch(docId, expiry);
                }
            }).retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, timeout)).max(retries).build())
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends Boolean>>() {
                        @Override
                        public Observable<? extends Boolean> call(Throwable throwable) {
                            if (throwable instanceof DocumentDoesNotExistException) {
                                return Observable.empty();
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }).toBlocking().last();
        } catch (NoSuchElementException ex) {
            // no items returned
        }
    }
}
