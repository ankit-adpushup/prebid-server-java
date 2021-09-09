package org.prebid.server.auction;

import com.adpushup.e3.Database.DbManager;
// import com.adpushup.CustomSerializers.*;
import com.adpushup.e3.Database.Cache.DbCacheManager;
import com.adpushup.e3.Database.ElasticsearchManager;
import com.adpushup.LogEnums.LogLevel;
import com.adpushup.LogEnums.LogType;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.TextNode;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import com.iab.openrtb.response.Bid;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.FileHandler;
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
    private DbManager db;
    private DbCacheManager dbCache;
    private ExecutorService executor;
    private final String LogSource = "PrebidServer.AmpPostProcessor";
    private ElasticsearchManager esManager;
    private ObjectMapper bidResponseJsonMapperForLog;
    private final String priceGranularityJson = "{" + "\"precision\": 2," + "\"ranges\": [" + "{" + "\"min\": 0,"
    + "\"max\": 3," + "\"increment\": 0.01" + "}," + "{" + "\"min\": 3," + "\"max\": 8,"
    + "\"increment\": 0.05" + "}," + "{" + "\"min\": 8," + "\"max\": 20," + "\"increment\": 0.5" + "}"
    + "]" + "}";
    private final JsonObject priceGranularityObject = JsonObject.fromJson(priceGranularityJson);

    private void sendSystemLogEvent(LogLevel logLevel, String message, String detailedMessage, String jsonDebugData, LogType logType) {
        Runnable runnableTask = () -> {
            try {
                String logSource = LogSource + "." + logLevel;
                esManager.insertSystemLog(logLevel.getValue(), logSource, message, detailedMessage, jsonDebugData, logType.getValue());
            } catch(Exception e) {
                logger.error("Exception in send log task");
                e.printStackTrace();
            }
        };
        this.executor.execute(runnableTask);
    }

    private void sendSystemLogEvent(Exception ex) {
        Runnable runnableTask = () -> {
            try {
                esManager.insertSystemLog(LogSource+".ERROR", ex);
            } catch(Exception e) {
                logger.error("Exception in send log task");
                e.printStackTrace();
            }
        };
        this.executor.execute(runnableTask);
    }

    private void sendAuctionLogEvent(LogLevel logLevel, String message, String detailedMessage, String bidResJson, String impDataJson, LogType logType) {
        Runnable runnableTask = () -> {
            try {
                String logSource = LogSource + "." + logLevel;
                esManager.insertAuctionLog(logLevel.getValue(), logSource, message, detailedMessage, bidResJson, impDataJson, logType.getValue());
            } catch(Exception e) {
                logger.error("Exception in send log task");
                e.printStackTrace();
            }
        };
        this.executor.execute(runnableTask);
    }

    // private String getBidResponseJSONForLogging(BidResponse bidResponse) 
    // throws JsonProcessingException {
    //     return this.bidResponseJsonMapperForLog.writeValueAsString(bidResponse);
    // }

    public AdpushupAmpResponsePostProcessor(String imdFeedbackHost, String imdFeedbackEndpoint,
            String imdFeedbackCreativeEndpoint, String[] ips, String cbUsername, String cbPassword,
            String esHost, JacksonMapper mapper) {
        this.logger = LoggerFactory.getLogger(AdpushupAmpResponsePostProcessor.class);
        this.vertx = Vertx.vertx();
        this.httpClient = new BasicHttpClient(vertx, vertx.createHttpClient());
        this.mapper = Objects.requireNonNull(mapper);
        this.imdFeedbackHost = imdFeedbackHost;
        this.imdFeedbackEndpoint = imdFeedbackEndpoint;
        this.db = new DbManager(ips, cbUsername, cbPassword);
        this.dbCache = new DbCacheManager(51200, 300000 , db.getNewAppBucket(), db);
        this.esManager = new ElasticsearchManager(esHost);
        this.executor = Executors.newFixedThreadPool(100);

        // this.bidResponseJsonMapperForLog = new ObjectMapper();
        // this.bidResponseJsonMapperForLog.registerModule(new SimpleModule() {
        //     @Override
        //     public void setupModule(SetupContext context) {
        //         super.setupModule(context);
        //         context.addBeanSerializerModifier(new BidBeanSerializerModifier());
        //     }
        // });

        dbCache.queryAndSetCustomData(_bucket -> {
            String query1 = "SELECT ownerEmail, siteId, siteDomain from `AppBucket` WHERE meta().id like 'site::%';";
            String query2 = "SELECT adServerSettings.dfp.prebidGranularityMultiplier, adServerSettings.dfp.activeDFPCurrencyCode FROM `AppBucket`"
                    + " WHERE meta().id = 'user::%s';";
            String query3 = "SELECT RAW hbcf from `AppBucket` WHERE meta().id = 'hbdc::%s';";
            String query4 = "SELECT id, name, ad.networkData.dfpAdunit as networkAdUnitId FROM `AppBucket` WHERE meta().id like 'amtg::%s:%%'";
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
                    String siteDomain = Objects.toString(jsonObj.get("siteDomain"), "");
                    if(siteId.isEmpty()) {
                        logger.info("siteId is null or empty");
                        continue;
                    }
                    if(ownerEmail.isEmpty()) {
                        logger.info("ownerEmail is null or empty");
                        continue;
                    }
                    if(siteDomain.isEmpty()) {
                        logger.info("siteDomain is null or empty");
                        continue;
                    }
                    N1qlQueryResult userDocResult = _bucket.query(N1qlQuery.simple(String.format(query2, ownerEmail)));
                    List <N1qlQueryRow> rows = userDocResult.allRows();
                    if(rows.size() == 0) {
                        String message = "No user doc found for siteId=" + siteId + " and ownerEmail=" + ownerEmail;
                        logger.info(message);
                        sendSystemLogEvent(LogLevel.ERROR, "UserDoc not found", message, "", LogType.SLOG); 
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
                            JsonObject bidderObj = JsonObject.create();
                            bidderObj.put("bids", ((JsonObject) hbcf.get(bidder)).get("bids"));
                            bidderObj.put("revenueShare", ((JsonObject) hbcf.get(bidder)).get("revenueShare"));
                            revShareObj.put(bidder, bidderObj);
                        }
                        jsonObj.put("revenueShare", revShareObj);
                    }
                    JsonObject adUnits = JsonObject.create();
                    Integer adUnitsFound = 0;
                    for (N1qlQueryRow k : _bucket.query(N1qlQuery.simple(String.format(query4, siteId)))) {
                        adUnitsFound++;
                        JsonObject adUnitDetails = JsonObject.create();
                        adUnitDetails.put("sectionName", k.value().get("name"));
                        adUnitDetails.put("networkAdUnitId", k.value().get("networkAdUnitId"));
                        adUnits.put(k.value().get("id").toString(), adUnitDetails);
                    }
                    if(adUnitsFound > 0) {
                        logger.info(" siteId: " + siteId + " adUnits: " +  adUnits);
                    }
                    jsonObj.put("adUnits", adUnits);
                    jsonDoc = JsonDocument.create(siteId, jsonObj);
                    list.add(jsonDoc);
                }
            } catch (CouchbaseException e) {
                logger.info(e);
                sendSystemLogEvent(e);
            }
            logger.info("got " + list.size() + " items for cache");
            return list;
        });
    }

    private void sendApFeedback(Map<String, JsonNode> newTargeting, 
        BidRequest bidRequest, 
        String siteId, 
        String siteDomain, 
        String uuid, 
        String sectionId, 
        String sectionName, 
        String networkAdUnitId, 
        String activeDfpCurrencyCode, 
        String bidResJson,
        BigDecimal winningCpm) {
        try {
            Map<String, String> postBodyMap = new HashMap<String, String>();
            String json = mapper.encode(newTargeting);
            String pageUrl = bidRequest.getSite().getPage();
            String deviceUA = bidRequest.getDevice().getUa();
            String deviceIp = bidRequest.getDevice().getIp();
            logger.info("============= Device Ip: " + deviceIp);
            postBodyMap.put("siteId", siteId);
            postBodyMap.put("siteDomain", siteDomain);
            postBodyMap.put("pageUrl", pageUrl);
            postBodyMap.put("uuid", uuid);
            postBodyMap.put("sectionId", sectionId);
            postBodyMap.put("sectionName", sectionName);
            postBodyMap.put("networkAdUnitId", networkAdUnitId);
            postBodyMap.put("activeDfpCurrencyCode", activeDfpCurrencyCode);
            postBodyMap.put("targeting", json);
            postBodyMap.put("bidResponse", bidResJson);
            postBodyMap.put("deviceIp", deviceIp);
            postBodyMap.put("deviceUA", deviceUA);
            postBodyMap.put("winningCpm", winningCpm.toString());
            
            String postBody = mapper.encode(postBodyMap);
            
            httpClient.post(imdFeedbackHost + imdFeedbackEndpoint, HttpUtil.headers(), postBody, 1000L)
                .setHandler(res -> logger.info(res));
        } catch (Exception ex) {
            logger.info(ex);
            sendSystemLogEvent(ex);
        }
    }

    @Override
    public Future<AmpResponse> postProcess(BidRequest bidRequest, BidResponse bidResponse, AmpResponse ampResponse, RoutingContext context) {
        Map<String, JsonNode> newTargeting = ampResponse.getTargeting();
        try {
            String bidResJson = mapper.encode(bidResponse);
            String impDataJson = mapper.encode(bidRequest.getImp());
            int pbPrecision = priceGranularityObject.getInt("precision");
            JsonArray rangesArray = priceGranularityObject.getArray("ranges");
            String requestId = bidRequest.getId();
            String[] requestIdSplit = requestId.split(":", 2);
            String siteId = requestIdSplit[0];
            String sectionId = requestId;
            String sectionName = "";
            String networkAdUnitId = "";
            String activeDfpCurrencyCode = "USD";
            String siteDomain = "";
            JsonObject revShare = JsonObject.create();
            double granularityMultiplier = 1;
            BigDecimal winningCpm = new BigDecimal(0.0);
            String uuid = UUID.randomUUID().toString();
            boolean isGross = false;
            BigDecimal originalCpm = new BigDecimal(0.0);
            BigDecimal adjustedCpm = originalCpm;

            // Log bid response
            // seatbid[i].bid[i].adm holds the whole ad creative, which is too big and useless to log
            // so we remove it
            sendAuctionLogEvent(LogLevel.INFO, "BidResponse::"+requestId, "", bidResJson, impDataJson, LogType.AMPBidResponse); // Last parameter is the logType which is 1 for bidResponse

            try {
                JsonDocument customData = dbCache.getCustom(siteId);
                logger.info("customData.content()::" + customData.content());
                granularityMultiplier = Float
                        .parseFloat(customData.content().get("prebidGranularityMultiplier").toString());
                activeDfpCurrencyCode = customData.content().get("activeDFPCurrencyCode").toString();
                JsonObject adUnits = (JsonObject) customData.content().get("adUnits");
                JsonObject adUnitForSection = (JsonObject) adUnits.get(sectionId);
                sectionName = adUnitForSection.get("sectionName").toString();
                networkAdUnitId = adUnitForSection.get("networkAdUnitId").toString();
                siteDomain = customData.content().get("siteDomain").toString();
                revShare = (JsonObject) customData.content().get("revenueShare");
                logger.info("siteDomain=" + siteDomain);
                logger.info(customData.content().get("ownerEmail").toString());
                logger.info(customData.content().get("prebidGranularityMultiplier").toString());
                logger.info(sectionId);
                logger.info(sectionName);
                logger.info(networkAdUnitId);
                logger.info(revShare);
                logger.info("newTargeting" + newTargeting.toString());
            } catch (NullPointerException e) {
                logger.info(e);
                logger.info("NullPointerException while getting data from cache or while parsing the data");
                e.printStackTrace();
                sendSystemLogEvent(e);
                // throw exception to stop further processing
                throw new NullPointerException(e.getMessage());
            }

            if (!newTargeting.isEmpty()) {
                try {
                    String winningBidder = newTargeting.remove("hb_bidder").asText();
                    double winningBidderRevShare;
                    String bidType = ((JsonObject) revShare.get(winningBidder)).get("bids").toString();
                    isGross = bidType.equals("gross");

                    logger.info(winningBidder);

                    try {
                        winningBidderRevShare = Double.valueOf(((JsonObject) revShare.get(winningBidder)).get("revenueShare").toString());
                    } catch (NumberFormatException | NullPointerException e) {
                        // TODO Handle the case where bidder key not present in revshare
                        winningBidderRevShare = 0;
                        logger.info(e);
                    }
                    newTargeting.put("hb_ap_bidder", TextNode.valueOf(winningBidder));
                    newTargeting.put("hb_ap_ran", TextNode.valueOf("1"));
                    newTargeting.put("hb_ap_siteid", TextNode.valueOf(siteId));
                    newTargeting.put("hb_ap_format_amp", TextNode.valueOf("banner"));
                    newTargeting.remove("hb_pb");
                    BigDecimal pow = BigDecimal.valueOf(Math.pow(10, pbPrecision + 2));
                    List<SeatBid> sbids = bidResponse.getSeatbid();
                    logger.info("=========== auction response =========");
                    for (SeatBid sbid : sbids) {
                        // logger.info("bid from " + sbid.getSeat() + ", cpm=" + sbid.getBid().get(0).getPrice().floatValue());
                        if (sbid.getSeat() == winningBidder) {
                            originalCpm = sbid.getBid().get(0).getPrice();
                            adjustedCpm = originalCpm;
                            if(isGross) {
                                adjustedCpm = originalCpm
                                    .subtract(originalCpm.multiply(BigDecimal.valueOf(winningBidderRevShare / 100)));
                            }
                            BigDecimal max;
                            BigDecimal min;
                            BigDecimal increment;
                            BigDecimal pb = BigDecimal.ZERO;
                            JsonObject largestBucket = (JsonObject) rangesArray.get(rangesArray.size() - 1);
                            BigDecimal largestMax = new BigDecimal(largestBucket.get("max").toString()).multiply(BigDecimal.valueOf(granularityMultiplier));
                            if (adjustedCpm.compareTo(largestMax) > 0) {
                                pb = largestMax;
                            } else {
                                logger.info("===========TEST===========");
                                logger.info(granularityMultiplier);
                                for (Object s : rangesArray) {
                                    max = new BigDecimal(((JsonObject) s).get("max").toString()).multiply(BigDecimal.valueOf(granularityMultiplier));
                                    logger.info("InsideLoop");
                                    logger.info("max: " + max);
                                    min = new BigDecimal(((JsonObject) s).get("min").toString()).multiply(BigDecimal.valueOf(granularityMultiplier));
                                    logger.info("min: " + min);
                                    increment = new BigDecimal(((JsonObject) s).get("increment").toString()).multiply(BigDecimal.valueOf(granularityMultiplier));
                                    logger.info("increment: " + increment);
                                    logger.info("adjustedCpm: " + adjustedCpm);
                                    if (adjustedCpm.compareTo(max) < 0 && adjustedCpm.compareTo(min) >= 0) {
                                        logger.info("======INSIDE IF=====");
                                        logger.info(adjustedCpm);
                                        logger.info(max);
                                        logger.info(min);
                                        int cpmToFloor = ((adjustedCpm.multiply(pow).subtract(min.multiply(pow)))
                                                .divide(increment.multiply(pow), RoundingMode.DOWN)).intValue();
                                        pb = increment.multiply(BigDecimal.valueOf(cpmToFloor)).add(min);
                                        break;
                                    }
                                }
                            }

                            DecimalFormat df = new DecimalFormat("0.00");
                            String apPb = df.format(pb);
                            newTargeting.put("hb_ap_pb_amp", TextNode.valueOf(apPb));

                            // AdId is not needed in amp ads, and bidders are not sending it either
                            // So, we will just send the bid.id
                            String AdId = sbid.getBid().get(0).getId();
                            newTargeting.put("hb_ap_adid", TextNode.valueOf(AdId));
                            logger.info("hb_ap_adid=" + AdId);
                            break;
                        }
                    }
                    logger.info("======================================");
                    newTargeting.put("hb_ap_auction_id", TextNode.valueOf(uuid));
                } catch(Exception ex) {
                    logger.info(ex);
                    ex.printStackTrace();
                    sendSystemLogEvent(ex);
                }
            }
            winningCpm = isGross ? adjustedCpm : originalCpm;

            logger.info("============X-Real-IP: " + context.request().headers().get("X-Real-IP"));
            sendApFeedback(newTargeting, bidRequest, siteId, siteDomain, uuid, sectionId, sectionName, networkAdUnitId, activeDfpCurrencyCode, bidResJson, winningCpm);
        } catch (Exception e) {
            logger.info("Generic Exception Caught");
            logger.info(e);
            e.printStackTrace();
            sendSystemLogEvent(e);
            newTargeting = new HashMap<String, JsonNode>();
            return Future.succeededFuture(AmpResponse.of(newTargeting, ampResponse.getDebug(), ampResponse.getErrors()));
        }
        return Future.succeededFuture(ampResponse);
    }
}
