package com.adpushup;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.adpushup.e3.Database.ElasticsearchManager;

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

    private void sendSystemLogEvent(LogEnums.LogLevel logLevel, String message, String detailedMessage, String jsonDebugData,
            LogEnums.LogType logType) {
        Runnable runnableTask = () -> {
            try {
                String logSource = LogSource + "." + logLevel;
                esManager.insertSystemLog(logLevel.getValue(), logSource, message, detailedMessage, jsonDebugData,
                        logType.getValue());
            } catch (Exception e) {
                logger.error("Exception in send log task");
                e.printStackTrace();
            }
        };
        this.executor.execute(runnableTask);
    }

    private void sendSystemLogEvent(Exception ex) {
        Runnable runnableTask = () -> {
            try {
                esManager.insertSystemLog(LogSource + ".ERROR", ex);
            } catch (Exception e) {
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
            sendSystemLogEvent(LogEnums.LogLevel.INFO, LogMessage, "", bidResJson, LogEnums.LogType.AuctionBidResponse);
        }
    }
}
