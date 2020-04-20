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

public class TimeDelayViewReader {

    // variables
    final Bucket _bucket;
    final String _designName;
    final String _viewName;
    final long _timeDelay;

    final Object _lock = new Object();
    long _lastKey;
    String _lastKeyDocId;
    long _endTimeLimit;
    volatile boolean _isEmpty = false;
    Stale _stale = Stale.TRUE;

    // constructor
    public TimeDelayViewReader(Bucket bucket, String designName, String viewName, long timeDelay) {
        _bucket = bucket;
        _designName = designName;
        _viewName = viewName;
        _timeDelay = timeDelay;

        _endTimeLimit = System.currentTimeMillis() - timeDelay;
    }

    // public
    public List<String> getNext(int count, long timeout) {
        List<String> docIdList = new ArrayList<>(count);

        synchronized (_lock) {
            if (!_isEmpty) {
                ViewResult result;

                if (_lastKeyDocId == null) {
                    result = _bucket.query(ViewQuery.from(_designName, _viewName).limit(count).stale(Stale.FALSE)
                            .reduce(false).startKey(_lastKey).endKey(_endTimeLimit), timeout, TimeUnit.MILLISECONDS);
                } else {
                    result = _bucket.query(
                            ViewQuery.from(_designName, _viewName).limit(count).stale(_stale).reduce(false)
                                    .startKey(_lastKey).startKeyDocId(_lastKeyDocId).endKey(_endTimeLimit).skip(1),
                            timeout, TimeUnit.MILLISECONDS);

                    if (_stale == Stale.FALSE) {
                        _stale = Stale.TRUE;
                    }
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
            _endTimeLimit = System.currentTimeMillis() - _timeDelay;

            _lastKey = 0;
            _lastKeyDocId = null;
            _isEmpty = false;
        }
    }

    public void partialReset() {
        synchronized (_lock) {
            _endTimeLimit = System.currentTimeMillis() - _timeDelay;

            _isEmpty = false;
            _stale = Stale.FALSE;
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
