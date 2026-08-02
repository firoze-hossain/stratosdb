package com.stratosdb.index;

import com.stratosdb.storage.page.BTreePage;

import java.util.List;

/**
 * What BTreeIndex and HashIndex have in common: point insert, point
 * delete by exact (key, RID), and point search (single or all matches
 * for a duplicate key). Range scanning is deliberately NOT part of this
 * interface - hashing destroys key order on purpose, so only BTreeIndex
 * can offer it. A caller that specifically needs a range scan checks
 * `instanceof BTreeIndex` rather than this interface gaining a method
 * half its implementations would have to reject.
 */
public interface KeyValueIndex {
    void insert(long key, BTreePage.RID rid);

    void delete(long key, BTreePage.RID rid);

    BTreePage.RID search(long key);

    List<BTreePage.RID> searchAll(long key);
}
