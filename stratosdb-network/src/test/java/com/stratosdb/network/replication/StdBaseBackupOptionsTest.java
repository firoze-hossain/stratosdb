package com.stratosdb.network.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StdBaseBackupOptionsTest {

    @Test
    void parseBareCommand() {
        StdBaseBackupOptions options = StdBaseBackupOptions.parse("BASE_BACKUP");
        assertNotNull(options.label());
        assertTrue(options.label().startsWith("std_basebackup_"));
        assertFalse(options.noWait());
        assertEquals(0, options.maxRateBytesPerSec());
    }

    @Test
    void parseParenthesizedOptions() {
        String sql = "BASE_BACKUP (LABEL 'weekly_backup', FAST, WAL, MAX_RATE 2048, NOWAIT)";
        StdBaseBackupOptions options = StdBaseBackupOptions.parse(sql);
        assertEquals("weekly_backup", options.label());
        assertTrue(options.fast());
        assertTrue(options.wal());
        assertTrue(options.noWait());
        assertEquals(2048 * 1024L, options.maxRateBytesPerSec());
    }

    @Test
    void parseUnparenthesizedOptions() {
        String sql = "BASE_BACKUP LABEL 'nightly' PROGRESS FAST WAL MAX_RATE 512;";
        StdBaseBackupOptions options = StdBaseBackupOptions.parse(sql);
        assertEquals("nightly", options.label());
        assertTrue(options.progress());
        assertTrue(options.fast());
        assertTrue(options.wal());
        assertEquals(512 * 1024L, options.maxRateBytesPerSec());
    }
}
