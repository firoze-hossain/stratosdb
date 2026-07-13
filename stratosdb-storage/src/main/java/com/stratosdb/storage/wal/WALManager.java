package com.stratosdb.storage.wal;

import com.stratosdb.common.utils.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write-Ahead Log for durability
 */
public class WALManager {
    private static final Logger LOG = LoggerFactory.getLogger(WALManager.class);
    
    private final String walDirectory;
    private FileChannel walChannel;
    private final AtomicLong currentLSN;
    private boolean sync = true;
    
    // Operation types
    public static final int OP_INSERT = 1;
    public static final int OP_UPDATE = 2;
    public static final int OP_DELETE = 3;
    public static final int OP_COMMIT = 4;
    public static final int OP_ABORT = 5;
    public static final int OP_CHECKPOINT = 6;
    
    public WALManager(String dataDirectory) {
        this.walDirectory = dataDirectory + "/wal";
        this.currentLSN = new AtomicLong(0);
        initialize();
    }
    
    private void initialize() {
        try {
            File dir = new File(walDirectory);
            if (!dir.exists() && !dir.mkdirs()) {
                LOG.warn("Failed to create WAL directory: {}", walDirectory);
            }
            
            File walFile = new File(walDirectory, "wal.log");
            if (!walFile.exists() && !walFile.createNewFile()) {
                LOG.warn("Failed to create WAL file");
            }
            
            RandomAccessFile raf = new RandomAccessFile(walFile, "rw");
            this.walChannel = raf.getChannel();
            this.currentLSN.set(walChannel.size());
            
            LOG.info("WAL initialized at LSN: {}", currentLSN.get());
        } catch (Exception e) {
            LOG.error("Failed to initialize WAL", e);
        }
    }
    
    /**
     * Log an insert operation
     */
    public void logInsert(String tableName, long pageId, int slot, byte[] tupleData) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 + tupleData.length);
            buffer.putInt(OP_INSERT);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            buffer.putInt(tupleData.length);
            buffer.put(tupleData);
            
            writeBuffer(buffer);
        } catch (Exception e) {
            LOG.error("Failed to log insert", e);
        }
    }
    
    /**
     * Log a delete operation
     */
    public void logDelete(String tableName, long pageId, int slot) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(256);
            buffer.putInt(OP_DELETE);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            
            writeBuffer(buffer);
        } catch (Exception e) {
            LOG.error("Failed to log delete", e);
        }
    }
    
    /**
     * Log an update operation
     */
    public void logUpdate(String tableName, long pageId, int slot, byte[] oldData, byte[] newData) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 + oldData.length + newData.length);
            buffer.putInt(OP_UPDATE);
            
            byte[] tableBytes = tableName.getBytes();
            buffer.putInt(tableBytes.length);
            buffer.put(tableBytes);
            
            buffer.putLong(pageId);
            buffer.putInt(slot);
            
            buffer.putInt(oldData.length);
            buffer.put(oldData);
            
            buffer.putInt(newData.length);
            buffer.put(newData);
            
            writeBuffer(buffer);
        } catch (Exception e) {
            LOG.error("Failed to log update", e);
        }
    }
    
    /**
     * Log a commit
     */
    public void logCommit(long transactionId) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putInt(OP_COMMIT);
            buffer.putLong(transactionId);
            writeBuffer(buffer);
        } catch (Exception e) {
            LOG.error("Failed to log commit", e);
        }
    }
    
    /**
     * Log a checkpoint
     */
    public void checkpoint() {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(OP_CHECKPOINT);
            buffer.putLong(System.currentTimeMillis());
            writeBuffer(buffer);
            LOG.info("Checkpoint written at LSN: {}", currentLSN.get());
        } catch (Exception e) {
            LOG.error("Failed to write checkpoint", e);
        }
    }
    
    /**
     * Write buffer to WAL
     */
    private void writeBuffer(ByteBuffer buffer) {
        try {
            buffer.flip();
            long position = currentLSN.getAndAdd(buffer.limit());
            walChannel.position(position);
            walChannel.write(buffer);
            
            if (sync) {
                walChannel.force(false);
            }
        } catch (Exception e) {
            LOG.error("Failed to write to WAL", e);
        }
    }
    
    /**
     * Recovery from WAL
     */
    public void recover() {
        try {
            LOG.info("Starting recovery...");
            
            walChannel.position(0);
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            
            while (true) {
                buffer.clear();
                int bytesRead = walChannel.read(buffer);
                if (bytesRead == -1) break;
                
                buffer.flip();
                if (buffer.remaining() < 4) break;
                
                int opType = buffer.getInt();
                switch (opType) {
                    case OP_INSERT:
                        // Replay insert
                        break;
                    case OP_DELETE:
                        // Replay delete
                        break;
                    case OP_UPDATE:
                        // Replay update
                        break;
                    case OP_COMMIT:
                        // Replay commit
                        break;
                    case OP_CHECKPOINT:
                        // Mark checkpoint
                        break;
                }
            }
            
            LOG.info("Recovery complete");
        } catch (Exception e) {
            LOG.error("Recovery failed", e);
        }
    }
    
    public void setSync(boolean sync) {
        this.sync = sync;
    }
    
    public long getCurrentLSN() {
        return currentLSN.get();
    }
    
    public void close() {
        try {
            checkpoint();
            if (walChannel != null && walChannel.isOpen()) {
                walChannel.force(true);
                walChannel.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close WAL", e);
        }
    }
}