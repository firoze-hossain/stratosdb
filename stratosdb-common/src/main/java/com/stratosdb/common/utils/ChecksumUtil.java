package com.stratosdb.common.utils;

import java.util.zip.CRC32;

public class ChecksumUtil {
    
    public static long calculateCRC32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }
    
    public static boolean verifyCRC32(byte[] data, long expectedChecksum) {
        return calculateCRC32(data) == expectedChecksum;
    }
}