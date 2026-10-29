package com.stratosdb.storage.buffer;

import com.stratosdb.storage.page.Page;
import com.stratosdb.storage.page.PageFactory;

public interface BufferPool {
    Page getPage(String tableName, long pageId);
    <T extends Page> T getPage(String tableName, long pageId, PageFactory<T> factory);
    void markDirty(String tableName, long pageId);
    void unpinPage(String tableName, long pageId);
    void flushAll();
    void flushPage(String tableName, long pageId);
    void evictPage(String tableName, long pageId);
    double getCacheHitRatio();
    int getCacheSize();
    long getTablePageCount(String tableName);
    default com.stratosdb.storage.disk.DiskManager getDiskManager() {
        return null;
    }
    default boolean isOffHeap() {
        return true;
    }
    default long getOffHeapMemoryUsed() {
        return (long) getCacheSize() * com.stratosdb.common.constants.PageConstants.PAGE_SIZE;
    }
    default int getDirtyPageCount() {
        return 0;
    }
    default int flushDirtyPages(int maxPagesToFlush) {
        return 0;
    }
    default boolean isBackgroundFlusherRunning() {
        return false;
    }
    default void stopBackgroundFlusher() {}
    void close();
}