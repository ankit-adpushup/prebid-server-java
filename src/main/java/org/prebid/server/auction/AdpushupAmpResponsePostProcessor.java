package org.prebid.server.auction;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class AdpushupAmpResponsePostProcessor implements AmpResponsePostProcessor {

    private Vertx vertx;
    private BasicHttpClient httpClient;
    private JacksonMapper mapper;
    private Logger logger;
    private String imdFeedbackHost;
    private String imdFeedbackEndpoint;
    private String imdFeedbackCreativeEndpoint;

    public AdpushupAmpResponsePostProcessor(String imdFeedbackHost, String imdFeedbackEndpoint,
                                            String imdFeedbackCreativeEndpoint, JacksonMapper mapper) {
        this.logger = LoggerFactory.getLogger(AdpushupAmpResponsePostProcessor.class);
        this.vertx = Vertx.vertx();
        this.httpClient = new BasicHttpClient(vertx, vertx.createHttpClient());
        this.mapper = Objects.requireNonNull(mapper);
        this.imdFeedbackHost = imdFeedbackHost;
        this.imdFeedbackEndpoint = imdFeedbackEndpoint;
        this.imdFeedbackCreativeEndpoint = imdFeedbackCreativeEndpoint;
    }

    @Override
    public Future<AmpResponse> postProcess(BidRequest bidRequest, BidResponse bidResponse, AmpResponse ampResponse,
                                           RoutingContext context) {
        Map<String, JsonNode> newTargeting = ampResponse.getTargeting();
        if (!newTargeting.isEmpty()) {
            newTargeting.put("hb_ap_id", TextNode.valueOf(UUID.randomUUID().toString()));
            newTargeting.put("hb_ap_bidder", TextNode.valueOf(newTargeting.remove("hb_bidder").asText()));
            newTargeting.put("hb_ap_size", TextNode.valueOf(newTargeting.remove("hb_size").asText()));
            List<SeatBid> sbids = bidResponse.getSeatbid();
            String winningBidder = newTargeting.get("hb_ap_bidder").textValue();
            for (SeatBid sbid: sbids) {
                if (sbid.getSeat() == winningBidder) {
                    newTargeting.put("hb_ap_cpm", TextNode.valueOf(sbid.getBid().get(0).getPrice().toString()));
                }
            }
            newTargeting.put("hb_ap_feedback_url", TextNode.valueOf(imdFeedbackHost + imdFeedbackCreativeEndpoint));
            newTargeting.put("hb_ap_pb", TextNode.valueOf(newTargeting.remove("hb_pb").textValue()));
            try {
                String json = mapper.encode(newTargeting);
                String bidResJson = mapper.encode(bidResponse);
                Map<String, String> postBodyMap = new HashMap<String, String>();
                postBodyMap.put("targeting", json);
                postBodyMap.put("bidResponse", bidResJson);
                String postBody = mapper.encode(postBodyMap);
                Future<?> future = httpClient.post(imdFeedbackHost + imdFeedbackEndpoint,
                                                   HttpUtil.headers(), postBody, 1000L)
                                                   .setHandler(res -> logger.info(res));
            } catch (EncodeException e) {
                logger.info(e);
            }
        }
        return Future.succeededFuture(ampResponse);
    }
}
