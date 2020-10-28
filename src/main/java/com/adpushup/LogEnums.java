package com.adpushup;

public class LogEnums {
    public enum LogLevel {
        INFO(1),
        WARNING(2),
        ERROR(3),
        ERR(3),
        UNKNOWN(4),
        EXCEPTION(5);
        private final int level;
        LogLevel(final int level) {
            this.level = level;
        }
        public int getValue() {
            return level;
        }
    };
    public enum LogType {
        SLOG(0),
        AMPBidResponse(1),
        AuctionBidResponse(2);
        private final int level;
        LogType(final int level) {
            this.level = level;
        }
        public int getValue() {
            return level;
        }
    };
}