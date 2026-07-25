package com.stratosdb.storage.page;

/**
 * Lets the buffer pool stay page-type-agnostic. Different storage structures
 * (heap tables, B+Tree indexes, ...) lay out their 8KB pages completely
 * differently; the pool shouldn't need to know the difference, only how to
 * create an empty page of the right type and how to reconstitute one from
 * bytes read off disk.
 */
public interface PageFactory<T extends Page> {
    T createEmpty(long pageId);
    T wrap(long pageId, byte[] existingBytes);
}
