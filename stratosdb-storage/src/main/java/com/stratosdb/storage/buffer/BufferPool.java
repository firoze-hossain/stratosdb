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
    void close();
}