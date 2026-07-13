package com.stratosdb.common.utils;

public class DateTimeUtil {
    
    public static String formatTimestamp(long timestamp) {
        return java.time.Instant.ofEpochMilli(timestamp)
                .toString();
    }
    
    public static long parseTimestamp(String timestamp) {
        return java.time.Instant.parse(timestamp)
                .toEpochMilli();
    }
}