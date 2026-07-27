package com.stratosdb.storage.page;

import com.stratosdb.common.constants.PageConstants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Slotted page for heap tables
 */
public class SlottedPage extends Page {
    
    public SlottedPage(long pageId) {
        super(pageId);
    }
    
    public SlottedPage(long pageId, byte[] data) {
        super(pageId, data);
    }
    
    /**
     * Insert a tuple into the page
     * @return slot number if successful, -1 if page is full
     */
    public int insertTuple(byte[] tupleData) {
        ByteBuffer buf = getBuffer();
        int lower = buf.getShort(12);
        int upper = buf.getShort(14);
        
        int itemSize = 6 + tupleData.length; // item pointer (6 bytes) + tuple data
        
        if (upper - lower < itemSize) {
            return -1; // Page full
        }
        
        // Write tuple data from bottom up
        int newUpper = upper - tupleData.length;
        buf.position(newUpper);
        buf.put(tupleData);
        
        // Write item pointer at lower offset
        buf.position(lower);
        buf.putInt(newUpper); // offset to tuple
        buf.putShort(lower + 4, (short) tupleData.length);
        
        // Update headers
        buf.putShort(12, (short) (lower + 6));
        buf.putShort(14, (short) newUpper);
        
        setDirty(true);
        
        // Return slot number (0-based)
        return (lower - 24) / 6;
    }
    
    /**
     * Read a tuple by slot number
     */
    public byte[] readTuple(int slot) {
        ByteBuffer buf = getBuffer();
        int itemOffset = 24 + (slot * 6);
        
        if (itemOffset >= buf.getShort(12)) {
            return null; // Invalid slot
        }
        
        int tupleOffset = buf.getInt(itemOffset);
        short tupleLength = buf.getShort(itemOffset + 4);
        
        if (tupleLength < 0) {
            return null; // Deleted tuple
        }
        
        byte[] tuple = new byte[tupleLength];
        buf.position(tupleOffset);
        buf.get(tuple);
        return tuple;
    }
    
    /**
     * Delete a tuple by slot
     */
    public void deleteTuple(int slot) {
        ByteBuffer buf = getBuffer();
        int itemOffset = 24 + (slot * 6);
        
        if (itemOffset >= buf.getShort(12)) {
            return;
        }
        
        // Mark as deleted by setting length to -1
        buf.putShort(itemOffset + 4, (short) -1);
        setDirty(true);
    }
    
    /**
     * Get all valid slots
     */
    public List<Integer> getValidSlots() {
        List<Integer> slots = new ArrayList<>();
        ByteBuffer buf = getBuffer();
        int lower = buf.getShort(12);
        
        int slot = 0;
        int offset = 24;
        while (offset < lower) {
            short length = buf.getShort(offset + 4);
            if (length > 0) {
                slots.add(slot);
            }
            slot++;
            offset += 6;
        }
        return slots;
    }
    
    /**
     * Update a tuple. When newData is the same length as what's already
     * stored, this updates in place - no relocation, no wasted slot. That
     * matters a lot in practice: an MVCC tombstone (same xmin, same
     * payload, only xmax changes - see MVCCVisibility.withXmax) is always
     * exactly the same length as what it replaces, and every DELETE or
     * UPDATE produces one. The previous "delete and reinsert" version of
     * this method wasted a fresh slot on literally every single
     * delete/update, permanently, even though the data fit perfectly back
     * into the space it already occupied - exactly the kind of bloat
     * vacuum (see defragment(), below) exists to undo, so it's worth not
     * creating in the first place where avoidable.
     *
     * A genuinely different-length update still falls back to delete+
     * reinsert, which does cost a slot - a real, smaller, remaining
     * inefficiency for that rarer case, not hidden here.
     */
    public boolean updateTuple(int slot, byte[] newData) {
        ByteBuffer buf = getBuffer();
        int itemOffset = 24 + (slot * 6);
        int lower = buf.getShort(12);
        if (itemOffset >= lower) {
            return false; // invalid slot
        }
        short oldLength = buf.getShort(itemOffset + 4);
        if (oldLength == newData.length) {
            int tupleOffset = buf.getInt(itemOffset);
            buf.position(tupleOffset);
            buf.put(newData);
            setDirty(true);
            return true;
        }
        deleteTuple(slot);
        int newSlot = insertTuple(newData);
        return newSlot != -1;
    }
    
    /**
     * Check if page has space for a tuple
     */
    public boolean hasSpace(byte[] tupleData) {
        return getFreeSpace() >= (6 + tupleData.length);
    }
    
    /**
     * Compacts out invalid (deleted) tuples' data, reclaiming the space
     * they occupied. This is what VACUUM (see ExecutorEngine.executeVacuum)
     * actually reclaims - deleteTuple() alone only marks a slot invalid,
     * it never frees the bytes.
     *
     * Item pointer slots are NOT renumbered or removed here - a deleted
     * slot's item pointer entry stays exactly where it is (still marked
     * invalid). Only valid tuples' data bytes get repacked, closing the
     * gaps deleted tuples' data used to leave behind. This distinction
     * matters: index entries and any other external reference identify a
     * tuple by (pageId, slot) - renumbering slots here would silently
     * invalidate every such reference. Compacting the underlying bytes
     * while every surviving tuple's slot number stays fixed does not.
     */
    public void defragment() {
        ByteBuffer buf = getBuffer();
        int lower = buf.getShort(12);
        int slotCount = (lower - 24) / 6;

        record ValidTuple(int slot, int offset, short length) {}
        List<ValidTuple> valid = new ArrayList<>();
        for (int slot = 0; slot < slotCount; slot++) {
            int itemOffset = 24 + slot * 6;
            short length = buf.getShort(itemOffset + 4);
            if (length > 0) {
                valid.add(new ValidTuple(slot, buf.getInt(itemOffset), length));
            }
        }
        if (valid.isEmpty()) {
            buf.putShort(14, (short) PageConstants.PAGE_SIZE);
            setDirty(true);
            return;
        }

        // Tuple data is written from the bottom of the page upward, so the
        // tuple closest to the bottom has the smallest offset - repacking
        // in ascending-offset order and writing each into the new upper
        // boundary (working downward from PAGE_SIZE) reproduces that same
        // bottom-up layout with the gaps squeezed out.
        valid.sort((a, b) -> Integer.compare(a.offset(), b.offset()));

        byte[][] tupleBytes = new byte[valid.size()][];
        for (int i = 0; i < valid.size(); i++) {
            ValidTuple vt = valid.get(i);
            byte[] data = new byte[vt.length()];
            buf.position(vt.offset());
            buf.get(data);
            tupleBytes[i] = data;
        }

        int newUpper = PageConstants.PAGE_SIZE;
        for (int i = 0; i < valid.size(); i++) {
            ValidTuple vt = valid.get(i);
            newUpper -= vt.length();
            buf.position(newUpper);
            buf.put(tupleBytes[i]);
            buf.putInt(24 + vt.slot() * 6, newUpper); // same slot, new offset, same length
        }

        buf.putShort(14, (short) newUpper);
        setDirty(true);
    }
}