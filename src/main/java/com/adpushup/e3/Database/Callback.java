package com.adpushup.e3.Database;

import java.util.ArrayList;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;

public interface Callback {
  // It will return an ArrayList of JsonDocument and the queryAndSetCustomData function will iterate through and set the cache
  ArrayList<JsonDocument> call(Bucket _bucket);
}