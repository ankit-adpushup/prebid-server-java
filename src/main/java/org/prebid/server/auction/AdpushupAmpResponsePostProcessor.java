package org.prebid.server.auction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import org.prebid.server.proto.response.AmpResponse;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AdpushupAmpResponsePostProcessor implements AmpResponsePostProcessor {

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
            newTargeting.put("hb_ap_pb", TextNode.valueOf(newTargeting.remove("hb_pb").textValue()));
        }
        return Future.succeededFuture(ampResponse);
    }
}
