package com.adpushup.e3.Database;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ArrayRangeViewReader {

    // variables
    final Bucket _bucket;
    final String _designName;
    final String _viewName;
    JsonArray _startKey;
    JsonArray _endKey;

    final Object _lock = new Object();
    JsonArray _lastKey;
    String _lastKeyDocId;
    boolean _isEmpty = false;

    // constructor
    public ArrayRangeViewReader(Bucket bucket, String designName, String viewName, JsonArray startKey,
            JsonArray endKey) {
        _bucket = bucket;
        _designName = designName;
        _viewName = viewName;
        _startKey = startKey;
        _endKey = endKey;
    }

    // public
    public List<String> getNext(int count, long timeout) {
        List<String> docIdList = new ArrayList<>(count);

        synchronized (_lock) {
            if (!_isEmpty) {
                ViewResult result;

                if (_lastKey == null) {
                    result = _bucket.query(
                            ViewQuery.from(_designName, _viewName).limit(count).stale(Stale.FALSE).reduce(false)
                                    .startKey(_startKey).endKey(_endKey).inclusiveEnd(),
                            timeout, TimeUnit.MILLISECONDS);
                } else {
                    result = _bucket.query(ViewQuery.from(_designName, _viewName).limit(count).stale(Stale.TRUE)
                            .reduce(false).startKey(_lastKey).startKeyDocId(_lastKeyDocId).endKey(_endKey)
                            .inclusiveEnd().skip(1), timeout, TimeUnit.MILLISECONDS);
                }

                JsonArray currentLastKey = null;
                String currentLastKeyDocId = null;
                Iterator<ViewRow> rows = result.iterator();

                // read ids
                while (rows.hasNext()) {
                    ViewRow row = rows.next();

                    currentLastKey = (JsonArray) row.key();
                    currentLastKeyDocId = row.id();

                    docIdList.add(currentLastKeyDocId);
                }

                if (currentLastKey == null) {
                    _isEmpty = true;
                } else {
                    _lastKey = currentLastKey;
                    _lastKeyDocId = currentLastKeyDocId;
                }
            }
        }

        return docIdList;
    }

    public void reset() {
        synchronized (_lock) {
            _lastKey = null;
            _lastKeyDocId = null;
            _isEmpty = false;
        }
    }

    public void reset(JsonArray startKey, JsonArray endKey) {
        synchronized (_lock) {
            _startKey = startKey;
            _endKey = endKey;

            _lastKey = null;
            _lastKeyDocId = null;
            _isEmpty = false;
        }
    }

    public void setEmpty() {
        synchronized (_lock) {
            _isEmpty = true;
        }
    }

    public boolean isEmpty() {
        return _isEmpty;
    }
}
