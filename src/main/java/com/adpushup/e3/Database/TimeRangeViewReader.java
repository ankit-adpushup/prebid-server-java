package com.adpushup.e3.Database;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TimeRangeViewReader {

    // variables
    private final Bucket _bucket;
    private final String _designName;
    private final String _viewName;
    private long _startKey;
    private long _endKey;

    private final Object _lock = new Object();
    private long _lastKey;
    private String _lastKeyDocId;
    private boolean _isEmpty = false;

    // constructor
    public TimeRangeViewReader(Bucket bucket, String designName, String viewName, long startKey, long endKey) {
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

                if (_lastKeyDocId == null) {
                    result = _bucket.query(
                            ViewQuery.from(_designName, _viewName).limit(count).stale(Stale.FALSE).reduce(false)
                                    .startKey(_startKey).endKey(_endKey).inclusiveEnd(false),
                            timeout, TimeUnit.MILLISECONDS);
                } else {
                    result = _bucket.query(ViewQuery.from(_designName, _viewName).limit(count).stale(Stale.TRUE)
                            .reduce(false).startKey(_lastKey).startKeyDocId(_lastKeyDocId).endKey(_endKey)
                            .inclusiveEnd(false).skip(1), timeout, TimeUnit.MILLISECONDS);
                }

                long currentLastKey = 0;
                String currentLastKeyDocId = null;
                Iterator<ViewRow> rows = result.iterator();

                // read ids
                while (rows.hasNext()) {
                    ViewRow row = rows.next();

                    currentLastKey = (long) row.key();
                    currentLastKeyDocId = row.id();

                    docIdList.add(currentLastKeyDocId);
                }

                if (currentLastKeyDocId == null) {
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
            _lastKey = 0;
            _lastKeyDocId = null;
            _isEmpty = false;
        }
    }

    public void reset(long startKey, long endKey) {
        synchronized (_lock) {
            _startKey = startKey;
            _endKey = endKey;

            _lastKey = 0;
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
