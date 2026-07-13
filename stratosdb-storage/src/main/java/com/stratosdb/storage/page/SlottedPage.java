package com.stratosdb.storage.page;

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
     * Update a tuple
     */
    public boolean updateTuple(int slot, byte[] newData) {
        // For simplicity, delete and reinsert
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
     * Defragment the page - compact tuples and free space
     */
    public void defragment() {
        // Implementation would compact tuples to bottom
        // and rebuild item pointers at top
        // Complex but important for production
    }
}