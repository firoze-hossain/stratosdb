package com.stratosdb.storage.page;

import java.nio.ByteBuffer;

/**
 * Lets the buffer pool stay page-type-agnostic. Different storage structures
 * (heap tables, B+Tree indexes, ...) lay out their 8KB pages completely
 * differently; the pool shouldn't need to know the difference, only how to
 * create an empty page of the right type and how to reconstitute one from
 * bytes or direct off-heap buffers read off disk.
 */
public interface PageFactory<T extends Page> {
    T createEmpty(long pageId);
    T wrap(long pageId, byte[] existingBytes);

    default T wrap(long pageId, ByteBuffer directBuffer) {
        T page = createEmpty(pageId);
        ByteBuffer target = page.getBuffer();
        target.position(0);
        ByteBuffer src = directBuffer.duplicate();
        src.position(0);
        src.limit(Math.min(src.capacity(), target.capacity()));
        target.put(src);
        target.position(0);
        return page;
    }
}
