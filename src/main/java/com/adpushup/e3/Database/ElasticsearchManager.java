package com.adpushup.e3.Database;

import org.elasticsearch.client.RestHighLevelClient;

import java.net.InetAddress;

import com.couchbase.client.java.document.json.JsonObject;
import com.eaio.uuid.UUID;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;

public class ElasticsearchManager {
  private RestHighLevelClient client;
  private String _apRegion = System.getenv("AP_REGION");
  private String _localHostname;
  private String logsIndex;
  private String ampLogsIndex;

  public ElasticsearchManager(String esHost, String logsIndex, String ampLogsIndex) {
    this.logsIndex = logsIndex;
    this.ampLogsIndex = ampLogsIndex;
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
    this.client = new RestHighLevelClient(RestClient.builder(new HttpHost(esHost, 9200, "http")));
  }

  public void insert_doc(String index, JsonObject data) {
    IndexRequest request = new IndexRequest(index);
    request.source(data.toMap());
    client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
      @Override
      public void onResponse(IndexResponse indexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    });
  }

  public void insertSystemLog(int type, String source, String message, String details, String debugData) {
    String logKey = "slog::" + (new UUID()).toString();
    JsonObject json = JsonObject.create();
    JsonObject meta = JsonObject.create();
    String index = logsIndex;
    if (message.contains("BidResponse::")) {
      JsonObject debugDataObj = JsonObject.fromJson(debugData);
      json.put("debugData", debugDataObj);
      index = ampLogsIndex;
    } else {
      json.put("debugData", debugData);
    }
    meta.put("id", logKey);

    json.put("type", type);
    json.put("date", System.currentTimeMillis());
    json.put("source", source);
    json.put("message", message);
    json.put("details", details);
    json.put("hostname", _localHostname);
    json.put("region", _apRegion);
    json.put("meta", meta);

    insert_doc(index, json);
  }

  public void insertSystemLog(String source, Exception ex) {
    insertSystemLog(3, source, ex.toString(), DbManager.getStackTraceFromException(ex), null);
  }
}