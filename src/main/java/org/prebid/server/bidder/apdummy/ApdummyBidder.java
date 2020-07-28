package org.prebid.server.bidder.apdummy;

import org.prebid.server.bidder.OpenrtbBidder;
import org.prebid.server.json.JacksonMapper;

public class ApdummyBidder extends OpenrtbBidder<Void> {
  public ApdummyBidder(String endpoint, JacksonMapper mapper) {
    super(endpoint, RequestCreationStrategy.SINGLE_REQUEST, Void.class, mapper);
  }
}