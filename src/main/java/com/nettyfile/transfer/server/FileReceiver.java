package com.nettyfile.transfer.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 文件落盘的逻辑
 */
public class FileReceiver {

    private static final Logger log = LoggerFactory.getLogger(FileReceiver.class);

    /** 每向磁盘写入N个数据块后发送ack. */
    private static final int ACK_INTERVAL = 10;

    private final RandomAccessFile file;

    private final Path progressFile;
    private final long totalFileSize;
    private final Path outputDir;

    private long currentOffset = 0;
    private int chunksSinceLastAck = 0;
    private long totalBytesWritten = 0;
    private boolean closed = false;


    public FileReceiver(Path outputDir, String fileName, long totalFileSize, long resumeOffset)
            throws IOException {
        this.outputDir = outputDir;
        this.totalFileSize = totalFileSize;
        this.currentOffset = resumeOffset;
        Files.createDirectories(outputDir);
        String safeName = sanitizeFileName(fileName);
        Path filePath = outputDir.resolve(safeName);
        this.progressFile = outputDir.resolve(safeName + ".progress");
        // 打开文件，开始写入
        this.file = new RandomAccessFile(filePath.toFile(), "rw");
        if (resumeOffset > 0) {
            file.setLength(totalFileSize); // Pre-allocate to avoid fragmentation
            file.seek(resumeOffset);
            log.info("Resuming transfer of '{}' from offset {} (total: {} bytes)",
                    safeName, resumeOffset, totalFileSize);
        } else {
            // Pre-allocate the full file size to avoid fragmentation during writes
            file.setLength(totalFileSize);
            file.seek(0);
            log.info("Starting new transfer of '{}' (total: {} bytes)", safeName, totalFileSize);
        }
    }

    public void writeChunk(long offset, byte[] data, int chunkIndex) throws IOException {
        writeToDisk(offset, data, chunkIndex);
        long newOffset = offset + data.length;
        this.currentOffset = Math.max(this.currentOffset, newOffset);
        chunksSinceLastAck++;
    }


    public void writeOverflow(long offset, byte[] data, int chunkIndex) throws IOException {
        // currentOffset 未提前 — 它表示连续进度，
        // 磁盘上没有零散的字节。BitSet会跟踪这个数据块
        // “已在磁盘上”状态。
        writeToDisk(offset, data, chunkIndex);
    }


    public void syncContiguousOffset(long contiguousOffset) {
        if (contiguousOffset > this.currentOffset) {
            log.debug("Syncing contiguous offset: {} → {} ({} overflow chunks now linked)",
                    this.currentOffset, contiguousOffset,
                    (contiguousOffset - this.currentOffset) / (1024 * 1024));
            this.currentOffset = contiguousOffset;
        }
    }


    private void writeToDisk(long offset, byte[] data, int chunkIndex) throws IOException {
        if (closed) {
            throw new IOException("FileReceiver is closed");
        }
        file.seek(offset);
        file.write(data);
        totalBytesWritten += data.length;
        log.debug("Wrote chunk {} at offset {} ({} bytes), total written: {}",
                chunkIndex, offset, data.length, totalBytesWritten);
    }


    public boolean shouldSendAck() {
        return chunksSinceLastAck >= ACK_INTERVAL;
    }


    public void resetAckCounter() {
        chunksSinceLastAck = 0;
    }

    public void persistProgress() throws IOException {
        Files.writeString(progressFile, String.valueOf(currentOffset),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        log.debug("Progress persisted: {} / {} bytes", currentOffset, totalFileSize);
    }

    public boolean isComplete() {
        return currentOffset >= totalFileSize;
    }


    public double getProgressPercent() {
        if (totalFileSize == 0) return 100.0;
        return (currentOffset * 100.0) / totalFileSize;
    }

    public void close() {
        if (closed) return;
        closed = true;
        try {
            file.close();
            // Delete progress file on successful completion
            Files.deleteIfExists(progressFile);
            log.info("Transfer complete. File closed. Total written: {} bytes", totalBytesWritten);
        } catch (IOException e) {
            log.error("Error closing file", e);
        }
    }

    public void closeForResume() {
        if (closed) return;
        closed = true;
        try {
            file.close();
            log.info("Transfer interrupted. Progress saved at offset {}.", currentOffset);
        } catch (IOException e) {
            log.error("Error closing file", e);
        }
    }


    public long getCurrentOffset() {
        return currentOffset;
    }

    public long getTotalFileSize() {
        return totalFileSize;
    }

    public int getChunksSinceLastAck() {
        return chunksSinceLastAck;
    }

    /**
     * @param outputDir the output directory
     * @param fileName  the file name
     * @return resume offset in bytes, or 0 if no progress file exists
     */
    public static long getResumeOffset(Path outputDir, String fileName) {
        Path progressFile = outputDir.resolve(sanitizeFileName(fileName) + ".progress");
        try {
            if (Files.exists(progressFile)) {
                String content = Files.readString(progressFile).trim();
                long offset = Long.parseLong(content);
                log.info("Found progress file for '{}': resume at offset {}", fileName, offset);
                return offset;
            }
        } catch (IOException | NumberFormatException e) {
            log.warn("Failed to read progress file for '{}': {}", fileName, e.getMessage());
        }
        return 0;
    }

    /**
     * 对文件名进行清理，以防止路径遍历攻击。
     */
    static String sanitizeFileName(String name) {
        if (name == null || name.isEmpty()) {
            return "unnamed_file";
        }

        String safe = name.replaceAll("[/\\\\:\0]", "_");

        while (safe.startsWith(".")) {
            safe = safe.substring(1);
        }
        if (safe.isEmpty()) {
            return "unnamed_file";
        }
        return safe;
    }
}
