package org.prebid.server.auction;

import com.adpushup.e3.Database.DbManager;
import com.adpushup.e3.Database.Cache.DbCacheManager;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import org.prebid.server.json.EncodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.response.AmpResponse;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.vertx.http.BasicHttpClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class AdpushupAmpResponsePostProcessor implements AmpResponsePostProcessor {

    private Vertx vertx;
    private BasicHttpClient httpClient;
    private JacksonMapper mapper;
    private Logger logger;
    private String imdFeedbackHost;
    private String imdFeedbackEndpoint;
    private String imdFeedbackCreativeEndpoint;
    private DbManager db;
    private DbCacheManager dbCache;

    public AdpushupAmpResponsePostProcessor(String imdFeedbackHost, String imdFeedbackEndpoint,
            String imdFeedbackCreativeEndpoint, String[] ips, String cbUsername, String cbPassword,
            JacksonMapper mapper) {
        this.logger = LoggerFactory.getLogger(AdpushupAmpResponsePostProcessor.class);
        this.vertx = Vertx.vertx();
        this.httpClient = new BasicHttpClient(vertx, vertx.createHttpClient());
        this.mapper = Objects.requireNonNull(mapper);
        this.imdFeedbackHost = imdFeedbackHost;
        this.imdFeedbackEndpoint = imdFeedbackEndpoint;
        this.imdFeedbackCreativeEndpoint = imdFeedbackCreativeEndpoint;
        this.db = new DbManager(ips, cbUsername, cbPassword);
        this.dbCache = new DbCacheManager(51200, 30000, db.getNewAppBucket(), db);
        ArrayList<JsonDocument> docList = dbCache.queryAndSetCustomData(_bucket -> {
            String query1 = "SELECT ownerEmail, siteId from `AppBucket` WHERE meta().id like 'site::%';";
            String query2 = "SELECT adServerSettings.dfp.prebidGranularityMultiplier FROM `AppBucket`"
                    + " WHERE meta().id = 'user::%s';";
            String query3 = "SELECT RAW hbcf from `AppBucket` WHERE meta().id = 'hbdc::%s';";
            JsonObject jsonObj;
            JsonDocument jsonDoc;
            ArrayList<JsonDocument> list = new ArrayList<JsonDocument>();
            for (N1qlQueryRow row : _bucket.query(N1qlQuery.simple(query1))) {
                jsonObj = row.value();
                String siteId = jsonObj.get("siteId").toString();
                for (N1qlQueryRow i : _bucket
                        .query(N1qlQuery.simple(String.format(query2, jsonObj.get("ownerEmail").toString())))) {
                    jsonObj.put("prebidGranularityMultiplier", i.value().get("prebidGranularityMultiplier"));
                }
                for (N1qlQueryRow j : _bucket.query(N1qlQuery.simple(String.format(query3, siteId)))) {
                    JsonObject hbcf = j.value();
                    JsonObject revShareObj = JsonObject.create();
                    Set<String> bidderNames = hbcf.getNames();
                    for (String bidder : bidderNames) {
                        revShareObj.put(bidder, ((JsonObject) hbcf.get(bidder)).get("revenueShare"));
                    }
                    jsonObj.put("revenueShare", revShareObj);
                }
                jsonDoc = JsonDocument.create(siteId, jsonObj);
                list.add(jsonDoc);
            }
            return list;
        });
    }

    @Override
    public Future<AmpResponse> postProcess(BidRequest bidRequest, BidResponse bidResponse, AmpResponse ampResponse,
            RoutingContext context) {

        Map<String, JsonNode> newTargeting = ampResponse.getTargeting();
        String priceGranularityJson = "{"+
            "\"precision\": 2," +
            "\"ranges\": ["+
                "{"+
                    "\"min\": 0," +
                    "\"max\": 3," +
                    "\"increment\": 0.01" +
                "}," +
                "{" +
                    "\"min\": 3," +
                    "\"max\": 8," +
                    "\"increment\": 0.05" +
                "},"+
                "{" +
                    "\"min\": 8," +
                    "\"max\": 20," +
                    "\"increment\": 0.5" +
                "}" +
            "]" +
        "}";
        JsonObject priceGranularityObject = JsonObject.fromJson(priceGranularityJson);
        int pbPrecision = priceGranularityObject.getInt("precision");
        JsonArray rangesArray = priceGranularityObject.getArray("ranges");       
        String requestId = bidRequest.getId();
        String siteId = requestId.split(":", 2)[0];
        JsonObject revShare = JsonObject.create();
        float granularityMultiplier = 1;

        try {
            JsonDocument customData = dbCache.getCustom(siteId);
            revShare = (JsonObject) customData.content().get("revenueShare");
            granularityMultiplier = Float.parseFloat(customData.content().get("prebidGranularityMultiplier").toString());
            logger.info(customData.content().get("ownerEmail").toString());
            logger.info(customData.content().get("prebidGranularityMultiplier").toString());
            logger.info(revShare); // Another JsonObject
        } catch (NullPointerException e) {
            logger.info(e);
            logger.info("NullPointerException while getting data from cache");
        }

        if (!newTargeting.isEmpty()) {
            String uuid = UUID.randomUUID().toString();
            String winningBidder = newTargeting.remove("hb_bidder").asText();
            int winningBidderRevShare;
            try {
                winningBidderRevShare = Integer.parseInt((String) revShare.get(winningBidder));
            } catch (NumberFormatException e) {
                winningBidderRevShare = 0;
            }
            newTargeting.put("hb_ap_bidder", TextNode.valueOf(winningBidder));
            newTargeting.put("hb_ap_ran", TextNode.valueOf("1"));
            newTargeting.put("hb_ap_siteid", TextNode.valueOf(siteId));
            newTargeting.put("hb_ap_format", TextNode.valueOf("banner"));
            newTargeting.remove("hb_pb");
            float pow = (float) Math.pow(10, pbPrecision+2);
            List<SeatBid> sbids = bidResponse.getSeatbid();
            for (SeatBid sbid : sbids) {
                if (sbid.getSeat() == winningBidder) {
                    float originalCpm = sbid.getBid().get(0).getPrice().floatValue();
                    float adjustedCpm = originalCpm - (originalCpm*winningBidderRevShare/100);
                    float max;
                    float min;
                    float increment;
                    float pb = 0;
                    for (Object s: rangesArray) {
                        max = Float.parseFloat(((JsonObject) s).get("max").toString());
                        min = Float.parseFloat(((JsonObject) s).get("min").toString());
                        increment = Float.parseFloat(((JsonObject) s).get("increment").toString());
                        if (adjustedCpm < max && adjustedCpm >= min) {
                            float cpmToFloor = ((adjustedCpm*pow) - (min*pow))/(increment*pow);
                            pb = min + (increment * Math.round(cpmToFloor));
                            break;
                        }
                    }
                    
                    DecimalFormat df = new DecimalFormat("0.00");
                    df.setRoundingMode(RoundingMode.DOWN);
                    String apPb = df.format(pb*granularityMultiplier);
                    newTargeting.put("hp_ap_pb", TextNode.valueOf(apPb));
                    newTargeting.put("hb_ap_cpm", TextNode.valueOf(Float.toString(adjustedCpm)));
                    newTargeting.put("hb_ap_adid", TextNode.valueOf(sbid.getBid().get(0).getAdid()));
                }
            }
            String apFeedbackUrl = String.format("%s?id=%s&sid=%s", imdFeedbackHost + imdFeedbackCreativeEndpoint, uuid, siteId);
            newTargeting.put("hb_ap_feedback_url", TextNode.valueOf(apFeedbackUrl));
            try {
                String json = mapper.encode(newTargeting);
                String bidResJson = mapper.encode(bidResponse);
                Map<String, String> postBodyMap = new HashMap<String, String>();
                postBodyMap.put("uuid", uuid);
                postBodyMap.put("targeting", json);
                postBodyMap.put("bidResponse", bidResJson);
                String postBody = mapper.encode(postBodyMap);
                Future<?> future = httpClient
                        .post(imdFeedbackHost + imdFeedbackEndpoint, HttpUtil.headers(), postBody, 1000L)
                        .setHandler(res -> logger.info(res));
            } catch (EncodeException e) {
                logger.info(e);
            }
        }
        return Future.succeededFuture(ampResponse);
    }
}
