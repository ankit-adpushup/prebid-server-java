package com.adpushup;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.adpushup.e3.Database.ElasticsearchManager;
import com.iab.openrtb.request.BidRequest;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.server.analytics.model.AuctionEvent;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.analytics.AnalyticsReporter;

public class AdpushupAnalyticsReporter implements AnalyticsReporter {
    private Logger logger;
    private ExecutorService executor;
    private final String LogSource = "PrebidServer.AdpushupAnalyticsReporter";
    private final String LogMessage = "AuctionEndpointBidResponse";
    private ElasticsearchManager esManager;
    private JacksonMapper mapper;

    private void sendAuctionLogEvent(LogEnums.LogLevel logLevel, String message, String detailedMessage, String bidResJson, String impDataJson, LogEnums.LogType logType) {
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

    public AdpushupAnalyticsReporter(JacksonMapper mapper, String esHost) {
        this.logger = LoggerFactory.getLogger(AdpushupAnalyticsReporter.class);
        this.executor = Executors.newFixedThreadPool(100);
        this.mapper = mapper;
        this.esManager = new ElasticsearchManager(esHost);
    }

    @Override
    public <T> void processEvent(T event) {
        if (event instanceof AuctionEvent) {
            AuctionEvent e = (AuctionEvent) event;
            String bidResJson = mapper.encode(e.getBidResponse());
            BidRequest bidReq = e.getAuctionContext().getBidRequest();
            String impArray = mapper.encode(bidReq.getImp());
            sendAuctionLogEvent(LogEnums.LogLevel.INFO, LogMessage, "", bidResJson, impArray, LogEnums.LogType.AuctionBidResponse);
        }
    }
}
