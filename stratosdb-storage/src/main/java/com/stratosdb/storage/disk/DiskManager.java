package com.stratosdb.storage.disk;

import com.stratosdb.common.constants.PageConstants;
import com.stratosdb.common.exceptions.StorageException;
import com.stratosdb.storage.page.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles all disk I/O operations
 */
public class DiskManager {
    private static final Logger LOG = LoggerFactory.getLogger(DiskManager.class);
    
    private final String dataDirectory;
    private final ConcurrentHashMap<String, FileChannel> openFiles;
    private final ConcurrentHashMap<String, Long> fileSizes;
    
    public DiskManager(String dataDirectory) {
        this.dataDirectory = dataDirectory;
        this.openFiles = new ConcurrentHashMap<>();
        this.fileSizes = new ConcurrentHashMap<>();
        ensureDirectoryExists();
    }
    
    private void ensureDirectoryExists() {
        File dir = new File(dataDirectory);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new StorageException("Failed to create data directory: " + dataDirectory);
            }
        }
    }
    
    /**
     * Read a page from disk
     */
    public Page readPage(String tableName, long pageId) {
        try {
            String fileName = getTableFileName(tableName);
            FileChannel channel = getFileChannel(fileName);
            
            ByteBuffer buffer = ByteBuffer.allocate(PageConstants.PAGE_SIZE);
            long position = pageId * PageConstants.PAGE_SIZE;
            
            // Check if file is large enough
            if (position >= channel.size()) {
                // Page doesn't exist yet
                return new Page(pageId);
            }
            
            channel.position(position);
            int bytesRead = channel.read(buffer);
            
            if (bytesRead != PageConstants.PAGE_SIZE) {
                LOG.warn("Partial read: {} bytes, expected {}", bytesRead, PageConstants.PAGE_SIZE);
                return new Page(pageId);
            }
            
            return new Page(pageId, buffer.array());
        } catch (Exception e) {
            LOG.error("Failed to read page {} from table {}", pageId, tableName, e);
            return new Page(pageId);
        }
    }
    
    /**
     * Write a page to disk
     */
    public void writePage(String tableName, Page page) {
        try {
            String fileName = getTableFileName(tableName);
            FileChannel channel = getFileChannel(fileName);
            
            ByteBuffer buffer = ByteBuffer.wrap(page.getBytes());
            long position = page.getPageId() * PageConstants.PAGE_SIZE;
            
            channel.position(position);
            channel.write(buffer);
            channel.force(false); // fsync
            
            page.setDirty(false);
            LOG.debug("Written page {} for table {}", page.getPageId(), tableName);
        } catch (Exception e) {
            LOG.error("Failed to write page {} for table {}", page.getPageId(), tableName, e);
           // throw new StorageException("Failed to write page", e);
            throw new StorageException("Failed to write page");
        }
    }
    
    /**
     * Append a new page to a table file
     */
    public long appendPage(String tableName, Page page) {
        try {
            String fileName = getTableFileName(tableName);
            FileChannel channel = getFileChannel(fileName);
            
            long position = channel.size();
            ByteBuffer buffer = ByteBuffer.wrap(page.getBytes());
            
            channel.position(position);
            channel.write(buffer);
            channel.force(false);
            
            long pageId = position / PageConstants.PAGE_SIZE;
            page.setDirty(false);
            
            LOG.debug("Appended page {} to table {}", pageId, tableName);
            return pageId;
        } catch (Exception e) {
            LOG.error("Failed to append page to table {}", tableName, e);
           // throw new StorageException("Failed to append page", e);
            throw new StorageException("Failed to append page");
        }
    }
    
    /**
     * Get or create file channel
     */
    private FileChannel getFileChannel(String fileName) throws Exception {
        return openFiles.computeIfAbsent(fileName, key -> {
            try {
                File file = new File(dataDirectory, key);
                if (!file.exists()) {
                    if (!file.createNewFile()) {
                        throw new StorageException("Failed to create file: " + key);
                    }
                }
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                return raf.getChannel();
            } catch (Exception e) {
                LOG.error("Failed to open file: {}", key, e);
                throw new StorageException("Failed to open file: " + key);
            }
        });
    }
    
    private String getTableFileName(String tableName) {
        return tableName + ".dat";
    }
    
    /**
     * Delete a table file
     */
    public void deleteTable(String tableName) {
        try {
            String fileName = getTableFileName(tableName);
            FileChannel channel = openFiles.remove(fileName);
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            
            File file = new File(dataDirectory, fileName);
            if (file.exists() && !file.delete()) {
                LOG.warn("Failed to delete file: {}", fileName);
            }
            
            fileSizes.remove(fileName);
            LOG.info("Deleted table: {}", tableName);
        } catch (Exception e) {
            LOG.error("Failed to delete table {}", tableName, e);
           // throw new StorageException("Failed to delete table: " + tableName, e);
            throw new StorageException("Failed to delete table: " + tableName);
        }
    }
    
    /**
     * Check if a table exists
     */
    public boolean tableExists(String tableName) {
        String fileName = getTableFileName(tableName);
        return new File(dataDirectory, fileName).exists();
    }
    
    /**
     * Get table file size in pages
     */
    public long getTablePageCount(String tableName) {
        try {
            String fileName = getTableFileName(tableName);
            FileChannel channel = getFileChannel(fileName);
            return channel.size() / PageConstants.PAGE_SIZE;
        } catch (Exception e) {
            LOG.warn("Failed to get page count for table {}", tableName);
            return 0;
        }
    }
    
    /**
     * Close all open files
     */
    public void close() {
        for (String fileName : openFiles.keySet()) {
            try {
                FileChannel channel = openFiles.remove(fileName);
                if (channel != null && channel.isOpen()) {
                    channel.force(true);
                    channel.close();
                }
            } catch (Exception e) {
                LOG.error("Failed to close file: {}", fileName, e);
            }
        }
        LOG.info("DiskManager closed");
    }
}