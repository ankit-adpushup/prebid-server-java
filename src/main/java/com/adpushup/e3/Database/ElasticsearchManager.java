package com.adpushup.e3.Database;

import org.elasticsearch.client.RestHighLevelClient;

import java.net.InetAddress;
import java.util.Map;
import java.util.logging.FileHandler;

import com.couchbase.client.java.document.json.JsonObject;
import com.eaio.uuid.UUID;

import org.apache.http.HttpHost;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ElasticsearchManager {
  private RestHighLevelClient client;
  private String _apRegion = System.getenv("AP_REGION");
  private String _localHostname;
  private ObjectMapper mapper = new ObjectMapper();
  private Logger bidResponseLogger = LoggerFactory.getLogger("bidresponse-logger");
  private Logger slogLogger = LoggerFactory.getLogger("slog-logger");

  public ElasticsearchManager(String esHost) {
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

  public void insert_doc(String index, ObjectNode data) {
    IndexRequest request = new IndexRequest(index);
    Map<String, Object> dataMap = mapper.convertValue(data, new TypeReference<Map<String, Object>>(){});
    request.source(dataMap);
    client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
      @Override
      public void onResponse(IndexResponse indexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    });
  }

  public void insertSystemLog(int type, String source, String message, String details, String debugData, int logType) {
    String logKey = "slog::" + (new UUID()).toString();
    ObjectNode json = JsonNodeFactory.instance.objectNode();
    json.put("type", type);
    json.put("date", System.currentTimeMillis());
    json.put("source", source);
    json.put("message", message);
    json.put("details", details);
    json.put("hostname", _localHostname);
    json.put("region", _apRegion);
    json.put("id", logKey);
    if (logType == 1) {
      try {
        ObjectNode debugDataJson;
        debugDataJson = (ObjectNode) new ObjectMapper().readTree(debugData);
        json.set("bidResponse", debugDataJson);

      } catch(JsonProcessingException  e ) {
        String debugDataJsonError = "Error Parsing debugData";
        json.put("bidResponse", debugDataJsonError);

      }
      bidResponseLogger.info(json);
    } else {
      json.put("debugData", debugData);
      slogLogger.info(json);
    }
  }

  public void insertSystemLog(String source, Exception ex) {
    insertSystemLog(3, source, ex.toString(), DbManager.getStackTraceFromException(ex), null, 0);
  }
}