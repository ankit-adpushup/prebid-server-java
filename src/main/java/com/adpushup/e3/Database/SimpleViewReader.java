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

public class SimpleViewReader {

    // variables
    final Bucket _bucket;
    final String _designName;
    final String _viewName;

    final Object _lock = new Object();
    String _lastKey;
    boolean _isEmpty = false;

    // constructor
    public SimpleViewReader(Bucket bucket, String designName, String viewName) {
        _bucket = bucket;
        _designName = designName;
        _viewName = viewName;
    }

    // public
    public List<String> getNext(int count, long timeout) {
        List<String> docIdList = new ArrayList<>(count);

        synchronized (_lock) {
            if (!_isEmpty) {
                ViewResult result;

                if (_lastKey == null) {
                    result = _bucket.query(
                            ViewQuery.from(_designName, _viewName).limit(count).stale(Stale.FALSE).reduce(false),
                            timeout, TimeUnit.MILLISECONDS);
                } else {
                    result = _bucket.query(ViewQuery.from(_designName, _viewName).limit(count).stale(Stale.TRUE)
                            .reduce(false).startKey(_lastKey).skip(1), timeout, TimeUnit.MILLISECONDS);
                }

                String currentLastKey = null;
                Iterator<ViewRow> rows = result.iterator();

                // read ids
                while (rows.hasNext()) {
                    ViewRow row = rows.next();

                    currentLastKey = (String) row.key();
                    docIdList.add(currentLastKey);
                }

                if (currentLastKey == null) {
                    _isEmpty = true;
                } else {
                    _lastKey = currentLastKey;
                }
            }
        }

        return docIdList;
    }

    public void reset() {
        synchronized (_lock) {
            _lastKey = null;
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
