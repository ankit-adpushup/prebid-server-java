package org.prebid.server.auction;

import com.adpushup.e3.Database.DbManager;
import com.adpushup.e3.Database.Cache.DbCacheManager;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
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
import java.math.MathContext;
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
        this.dbCache = new DbCacheManager(51200, 30000 , db.getNewAppBucket(), db);
        dbCache.queryAndSetCustomData(_bucket -> {
            String query1 = "SELECT ownerEmail, siteId from `AppBucket` WHERE meta().id like 'site::%';";
            String query2 = "SELECT adServerSettings.dfp.prebidGranularityMultiplier, adServerSettings.dfp.activeDFPCurrencyCode FROM `AppBucket`"
                    + " WHERE meta().id = 'user::%s';";
            String query3 = "SELECT RAW hbcf from `AppBucket` WHERE meta().id = 'hbdc::%s';";
            String query4 = "SELECT id, name FROM `AppBucket` WHERE meta().id like 'amtg::%s:%%'";
            JsonObject jsonObj;
            JsonDocument jsonDoc;
            ArrayList<JsonDocument> list = new ArrayList<JsonDocument>();
            try {
                for (N1qlQueryRow row : _bucket.query(N1qlQuery.simple(query1))) {
                    jsonObj = row.value();
                    if(jsonObj == null) {
                        logger.info("jsonObj is null");
                        continue;
                    }
                    String siteId = Objects.toString(jsonObj.get("siteId"), "");
                    String ownerEmail = Objects.toString(jsonObj.get("ownerEmail"), "");
                    if(siteId.isEmpty()) {
                        logger.info("siteId is null or empty");
                        continue;
                    }
                    if(ownerEmail.isEmpty()) {
                        logger.info("ownerEmail is null or empty");
                        continue;
                    }
                    N1qlQueryResult userDocResult = _bucket.query(N1qlQuery.simple(String.format(query2, ownerEmail)));
                    List <N1qlQueryRow> rows = userDocResult.allRows();
                    if(rows.size() == 0) {
                        logger.info("No user doc found for siteId=" + siteId + " and ownerEmail=" + ownerEmail);
                        continue;
                    }
                    N1qlQueryRow i = rows.get(0);
                    jsonObj.put("prebidGranularityMultiplier", i.value().get("prebidGranularityMultiplier"));
                    jsonObj.put("activeDFPCurrencyCode", i.value().get("activeDFPCurrencyCode"));
    
                    for (N1qlQueryRow j : _bucket.query(N1qlQuery.simple(String.format(query3, siteId)))) {
                        JsonObject hbcf = j.value();
                        JsonObject revShareObj = JsonObject.create();
                        Set<String> bidderNames = hbcf.getNames();
                        for (String bidder : bidderNames) {
                            revShareObj.put(bidder, ((JsonObject) hbcf.get(bidder)).get("revenueShare"));
                        }
                        jsonObj.put("revenueShare", revShareObj);
                    }
                    JsonObject adUnits = JsonObject.create();
                    for (N1qlQueryRow k : _bucket.query(N1qlQuery.simple(String.format(query4, siteId)))) {
                        adUnits.put(k.value().get("id").toString(), k.value().get("name"));
                    }
                    jsonObj.put("adUnits", adUnits);
                    jsonDoc = JsonDocument.create(siteId, jsonObj);
                    list.add(jsonDoc);
                }
            } catch (CouchbaseException e) {
                logger.info(e);
            }
            logger.info("got " + list.size() + " items for cache");
            return list;
        });
    }

    @Override
    public Future<AmpResponse> postProcess(BidRequest bidRequest, BidResponse bidResponse, AmpResponse ampResponse,
            RoutingContext context) {

        Map<String, JsonNode> newTargeting = ampResponse.getTargeting();
        try {
            String priceGranularityJson = "{" + "\"precision\": 2," + "\"ranges\": [" + "{" + "\"min\": 0,"
                    + "\"max\": 3," + "\"increment\": 0.01" + "}," + "{" + "\"min\": 3," + "\"max\": 8,"
                    + "\"increment\": 0.05" + "}," + "{" + "\"min\": 8," + "\"max\": 20," + "\"increment\": 0.5" + "}"
                    + "]" + "}";
            JsonObject priceGranularityObject = JsonObject.fromJson(priceGranularityJson);
            int pbPrecision = priceGranularityObject.getInt("precision");
            JsonArray rangesArray = priceGranularityObject.getArray("ranges");
            String requestId = bidRequest.getId();
            String[] requestIdSplit = requestId.split(":", 2);
            String siteId = requestIdSplit[0];
            String sectionId = requestIdSplit[1];
            String sectionName = "";
            String activeDfpCurrencyCode = "USD";
            JsonObject revShare = JsonObject.create();
            double granularityMultiplier = 1;

            try {
                JsonDocument customData = dbCache.getCustom(siteId);
                revShare = (JsonObject) customData.content().get("revenueShare");
                granularityMultiplier = Float
                        .parseFloat(customData.content().get("prebidGranularityMultiplier").toString());
                sectionName = ((JsonObject) customData.content().get("adUnits")).get(sectionId).toString();
                activeDfpCurrencyCode = customData.content().get("activeDFPCurrencyCode").toString();

                logger.info(customData.content().get("ownerEmail").toString());
                logger.info(customData.content().get("prebidGranularityMultiplier").toString());
                logger.info(sectionId);
                logger.info(sectionName);
                logger.info(revShare); // Another JsonObject
                logger.info("newTargeting" + newTargeting.toString());
            } catch (NullPointerException e) {
                logger.info(e);
                logger.info("NullPointerException while getting data from cache or while parsing the data");
                e.printStackTrace();
            }

            if (!newTargeting.isEmpty()) {
                String uuid = UUID.randomUUID().toString();
                String winningBidder = newTargeting.remove("hb_bidder").asText();
                double winningBidderRevShare;
                logger.info(winningBidder);
                try {
                    winningBidderRevShare = Double.parseDouble(((String) revShare.get(winningBidder)));
                } catch (NumberFormatException | NullPointerException e) { // TODO Handle the case where bidder key not present in revshare
                                                    // object
                    winningBidderRevShare = 0;
                    logger.info(e);
                }
                newTargeting.put("hb_ap_bidder", TextNode.valueOf(winningBidder));
                newTargeting.put("hb_ap_ran", TextNode.valueOf("1"));
                newTargeting.put("hb_ap_siteid", TextNode.valueOf(siteId));
                newTargeting.put("hb_ap_format", TextNode.valueOf("banner"));
                newTargeting.remove("hb_pb");
                BigDecimal pow = BigDecimal.valueOf(Math.pow(10, pbPrecision + 2));
                List<SeatBid> sbids = bidResponse.getSeatbid();
                logger.info("=========== auction response =========");
                for (SeatBid sbid : sbids) {
                    logger.info("bid from " + sbid.getSeat() + ", cpm=" + sbid.getBid().get(0).getPrice().floatValue());
                    if (sbid.getSeat() == winningBidder) {
                        BigDecimal originalCpm = sbid.getBid().get(0).getPrice();
                        BigDecimal adjustedCpm = originalCpm
                                .subtract(originalCpm.multiply(BigDecimal.valueOf(winningBidderRevShare / 100)));
                        BigDecimal max;
                        BigDecimal min;
                        BigDecimal increment;
                        BigDecimal pb = BigDecimal.ZERO;
                        JsonObject largestBucket = (JsonObject) rangesArray.get(rangesArray.size() - 1);
                        BigDecimal largestMax = new BigDecimal(largestBucket.get("max").toString());
                        if (adjustedCpm.compareTo(largestMax) > 0) {
                            pb = largestMax;
                        } else {
                            for (Object s : rangesArray) {
                                max = new BigDecimal(((JsonObject) s).get("max").toString());
                                min = new BigDecimal(((JsonObject) s).get("min").toString());
                                increment = new BigDecimal(((JsonObject) s).get("increment").toString());
                                if (adjustedCpm.compareTo(max) < 0 && adjustedCpm.compareTo(min) >= 0) {
                                    BigDecimal cpmToFloor = ((adjustedCpm.multiply(pow).subtract(min.multiply(pow)))
                                            .divide(increment.multiply(pow)));
                                    pb = cpmToFloor.round(new MathContext(2, RoundingMode.DOWN)).multiply(increment)
                                            .add(min);
                                    break;
                                }
                            }
                        }

                        DecimalFormat df = new DecimalFormat("0.00");
                        df.setRoundingMode(RoundingMode.DOWN);
                        String apPb = df.format(pb.multiply(BigDecimal.valueOf(granularityMultiplier)));
                        newTargeting.put("hp_ap_pb", TextNode.valueOf(apPb));
                        newTargeting.put("hb_ap_cpm", TextNode.valueOf(adjustedCpm.toString()));
                        newTargeting.put("hb_ap_adid", TextNode.valueOf(sbid.getBid().get(0).getAdid()));
                    }
                }
                logger.info("======================================");
                String apFeedbackUrl = String.format("%s?id=%s&sid=%s", imdFeedbackHost + imdFeedbackCreativeEndpoint,
                        uuid, siteId);
                newTargeting.put("hb_ap_feedback_url", TextNode.valueOf(apFeedbackUrl));
                try {
                    String json = mapper.encode(newTargeting);
                    String bidResJson = mapper.encode(bidResponse);
                    Map<String, String> postBodyMap = new HashMap<String, String>();
                    postBodyMap.put("uuid", uuid);
                    postBodyMap.put("sectionId", sectionId);
                    postBodyMap.put("sectionName", sectionName);
                    postBodyMap.put("activeDfpCurrencyCode", activeDfpCurrencyCode);
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

        } catch (Exception e) {
            logger.info("Generic Exception Caught");
            logger.info(e);
            newTargeting = new HashMap<String, JsonNode>();
            return Future.succeededFuture(AmpResponse.of(newTargeting, ampResponse.getDebug(), ampResponse.getErrors()));
        }
        return Future.succeededFuture(ampResponse);
    }
}
