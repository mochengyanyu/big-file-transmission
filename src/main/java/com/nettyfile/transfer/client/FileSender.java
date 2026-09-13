package com.nettyfile.transfer.client;

import com.nettyfile.transfer.protocol.FileTransferPacket;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * * 从磁盘读取文件，并通过Netty通道以1MB的块大小发送。
 * * <h3>特性</h3> *
 * <ul> * <li><b>速率限制</b>：采用令牌桶机制，限制在10 MB/s以内，以确保不超过80 Mbps的带宽预留。使用事件循环调度，永不阻塞。</li>
 * * <li><b>流量控制</b>：遵循Netty的通道可写性（高/低水位线）。当通道不可写时，发送方暂停，并在可写性监听器触发时恢复。</li>
 * * <li><b>重传</b>：为最近发送的块维护一个挂起块映射。当收到NACK时，使用RETRANSMIT标志重新发送丢失的块。</li>
 * * <li><b>恢复支持</b>：从给定的字节偏移量开始读取，实现断点恢复。</li>
 * * <li><b>基于ACK的清理</b>：当服务器确认字节数达到偏移量X时，X之前的所有块将从挂起映射中移除。</li>
 * * </ul>
 * * * <h3>非阻塞设计</h3>
 * * 每个块发送都是一个在Netty事件循环上调度的独立任务。
 * 将一个数据块写入通道后，会安排下一次发送。
 * 速率限制采用延迟调度而非使用Thread.sleep()方法，* 从而保持事件循环可用于I/O处理。
 */
public class FileSender {


    private static final Logger log = LoggerFactory.getLogger(FileSender.class);

    /** Chunk size: 1 MB. */
    public static final int CHUNK_SIZE = 1024 * 1024; // 1 MB

    /** 为重传保留的已发送但未确认的最大数据块数量 .
     *
     *  20 chunks × 1 MB = 20 MB memory. 这应该远远大于服务器的10块接收缓冲区.
     *
     * */
    private static final int MAX_PENDING_CHUNKS = 20;

    private final Channel channel;
    private final RandomAccessFile file;
    private final long fileId;
    private final String fileName;
    private final long fileSize;
    private final int totalChunks;
    private final RateLimiter rateLimiter;

    /** 文件中的当前读取位置 */
    private long currentOffset;

    /** 当前区块索引 */
    private int currentChunkIndex;

    /** 最近发送的保留用于潜在重传的数据块
     *  按插入顺序排序（LinkedHashMap），以便高效移除. */
    private final LinkedHashMap<Integer, PendingChunk> pendingChunks = new LinkedHashMap<>();

    /** 发送方是否因通道背压而暂停. */
    private volatile boolean paused;

    /** 是否所有数据块都已读取并发送. */
    private volatile boolean sendComplete;

    /** 传输是否已得到服务器的完全确认. */
    private volatile boolean transferComplete;

    /** 发送任务是否已安排（防止重复安排）. */
    private boolean sendScheduled;

    /**
     * @param channel      连接的Netty通道
     * @param filePath     要发送的文件的路径
     * @param fileId       唯一文件标识符
     * @param resumeOffset 开始发送的字节偏移量（新传输时为0）
     * @param rateLimiter  速率限制器（设为null表示禁用速率限制）
     * @throws IOException 如果文件无法读取
     */
    public FileSender(Channel channel, Path filePath, long fileId, long resumeOffset,
                      RateLimiter rateLimiter) throws IOException {
        this.channel = channel;
        this.fileId = fileId;
        this.fileName = filePath.getFileName().toString();
        this.rateLimiter = rateLimiter;

        this.file = new RandomAccessFile(filePath.toFile(), "r");
        this.fileSize = file.length();
        this.totalChunks = (int) ((fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE);
        this.currentOffset = resumeOffset;
        this.currentChunkIndex = (int) (resumeOffset / CHUNK_SIZE);

        log.info("FileSender initialized: '{}' ({} bytes, {} chunks), resume at offset {} (chunk {})",
                fileName, fileSize, totalChunks, resumeOffset, currentChunkIndex);
    }

    /**
     * 开始发送数据块。在通道的事件循环上安排第一次发送。
     */
    public void start() {
        scheduleNextSend(0);
    }

    /**
     * 延迟后安排发送下一个数据块。
     * 所有发送操作都通过事件循环进行，以避免并发访问。
     *
     * @param delayMs delay in milliseconds before sending (0 = immediate)
     */
    private void scheduleNextSend(long delayMs) {
        if (sendScheduled) return; // already queued
        if (sendComplete) return;
        sendScheduled = true;
        if (delayMs <= 0) {
            channel.eventLoop().execute(this::doSend);
        } else {
            channel.eventLoop().schedule(this::doSend, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 发送单个数据块。在事件循环上调用。
     * 发送后，要么安排下一个数据块，要么暂停。
     */
    private void doSend() {
        sendScheduled = false;

        // --- 警卫检查 ---
        if (sendComplete || transferComplete) return;

        if (paused) {
            // 流控制：通道缓冲区已满。请等待onChannelWritable()。
            log.debug("Paused (channel not writable), waiting for drain");
            return;
        }

        if (pendingChunks.size() >= MAX_PENDING_CHUNKS) {
            // 未确认的数据块过多。请等待确认（ACK）以释放空间。
            log.debug("Pending chunks full ({}), waiting for ACK", pendingChunks.size());
            return;
        }

        if (currentOffset >= fileSize) {
            sendComplete = true;
            log.info("All {} chunks sent ({} bytes). Awaiting final ACK.",
                    totalChunks, fileSize);
            return;
        }

        // --- 速率 限制 ---
        if (rateLimiter != null) {
            long waitMs = rateLimiter.acquireWithWaitTime(CHUNK_SIZE);
            if (waitMs > 0) {
                // Not enough tokens — reschedule after the wait
                scheduleNextSend(waitMs);
                return;
            }
        }

        // --- 读取并发送一个数据块 ---
        try {
            byte[] chunkData = readChunk();
            boolean isLast = (currentOffset + chunkData.length >= fileSize);

            FileTransferPacket packet = FileTransferPacket.data(
                    fileId, fileName, currentChunkIndex, currentOffset,
                    chunkData, isLast);

            // 在发送前存入待处理映射（用于重传）
            pendingChunks.put(currentChunkIndex,
                    new PendingChunk(currentChunkIndex, currentOffset, chunkData, isLast));

            // 发送数据包
            ChannelFuture future = channel.writeAndFlush(packet);
            future.addListener((ChannelFutureListener) f -> {
                if (!f.isSuccess()) {
                    log.error("Failed to send chunk {}: {}",
                            currentChunkIndex, f.cause().getMessage());
                }
            });

            log.debug("Sent chunk {} (offset {}, {} bytes){}",
                    currentChunkIndex, currentOffset, chunkData.length,
                    isLast ? " [LAST]" : "");

            currentOffset += chunkData.length;
            currentChunkIndex++;

            // --- Flow control check ---
            if (!channel.isWritable()) {
                paused = true;
                log.debug("Channel not writable (high watermark), pausing");
                return; // Wait for onChannelWritable() to resume
            }

            // --- Schedule next chunk ---
            // 使用execute()而不是直接调用doSend()以避免
            // 如果通道接受写入的速度非常快，则递归深度较大
            scheduleNextSend(0);

        } catch (IOException e) {
            log.error("Error reading file at offset {}: {}", currentOffset, e.getMessage(), e);
            channel.close();
        }
    }

    /**
     * 从文件中的当前偏移量读取一个数据块。
     */
    private byte[] readChunk() throws IOException {
        file.seek(currentOffset);
        int bytesToRead = (int) Math.min(CHUNK_SIZE, fileSize - currentOffset);
        byte[] buffer = new byte[bytesToRead];
        file.readFully(buffer);
        return buffer;
    }

    // ========================================================================
    // ACK / NACK handling
    // ========================================================================

    /**
     * 当从服务器收到确认（ACK）时调用。
     * 从待处理映射中移除已确认的数据块，并触发更多发送操作。
     *
     * @param receivedOffset all bytes up to this offset have been received
     */
    public void onAck(long receivedOffset) {
        // Remove pending chunks whose data ends before or at the acknowledged offset
        Iterator<Map.Entry<Integer, PendingChunk>> it = pendingChunks.entrySet().iterator();
        int removed = 0;
        while (it.hasNext()) {
            PendingChunk pc = it.next().getValue();
            if (pc.offset + pc.data.length <= receivedOffset) {
                it.remove();
                removed++;
            }
        }

        if (removed > 0) {
            log.debug("ACK offset={}: removed {} pending, {} remaining",
                    receivedOffset, removed, pendingChunks.size());
        }

        // Check for completion
        if (sendComplete && pendingChunks.isEmpty()) {
            transferComplete = true;
            log.info("Transfer complete! All {} chunks acknowledged.", totalChunks);
            FileTransferPacket complete = FileTransferPacket.complete(fileId, fileSize);
            channel.writeAndFlush(complete);
            return;
        }

        // Resume sending if we have room in the pending map
        if (!sendComplete && pendingChunks.size() < MAX_PENDING_CHUNKS) {
            scheduleNextSend(0);
        }
    }

    /**
     * 当从服务器收到NACK（否定确认）时调用。
     * 使用RETRANSMIT标志重新发送请求的数据块。
     */
    public void onNack(int missingChunkIndex) {
        PendingChunk pc = pendingChunks.get(missingChunkIndex);
        if (pc != null) {
            log.info("Retransmitting chunk {} (offset {}, {} bytes)",
                    pc.chunkIndex, pc.offset, pc.data.length);

            FileTransferPacket packet = FileTransferPacket.retransmit(
                    fileId, fileName, pc.chunkIndex, pc.offset, pc.data, pc.isLastChunk);

            channel.writeAndFlush(packet).addListener((ChannelFutureListener) f -> {
                if (!f.isSuccess()) {
                    log.error("Retransmit failed for chunk {}: {}",
                            pc.chunkIndex, f.cause().getMessage());
                }
            });
        } else {
            // Chunk evicted from pending map — re-read from file
            log.warn("NACK for chunk {} — not in pending (evicted). Re-reading from file.",
                    missingChunkIndex);
            retransmitFromFile(missingChunkIndex);
        }
    }

    /**
     * 回退：从文件中重新读取一个数据块以进行重传。
     */
    private void retransmitFromFile(int chunkIndex) {
        try {
            long offset = (long) chunkIndex * CHUNK_SIZE;
            if (offset >= fileSize) return;

            file.seek(offset);
            int bytesToRead = (int) Math.min(CHUNK_SIZE, fileSize - offset);
            byte[] data = new byte[bytesToRead];
            file.readFully(data);
            boolean isLast = (offset + data.length >= fileSize);

            FileTransferPacket packet = FileTransferPacket.retransmit(
                    fileId, fileName, chunkIndex, offset, data, isLast);
            channel.writeAndFlush(packet);
        } catch (IOException e) {
            log.error("Failed to re-read chunk {} for retransmission: {}", chunkIndex, e.getMessage());
        }
    }

    // ========================================================================
    // Flow control
    // ========================================================================

    /**
     * 当通道发生转换时，由{@link ClientChannelHandler}调用
     * 从不可写状态变为可写状态（写缓冲区已耗尽，低于低水位线）。
     */
    public void onChannelWritable() {
        if (paused) {
            paused = false;
            log.debug("Channel writable again, resuming send");
            scheduleNextSend(0);
        }
    }

    /**
     * Close the file handle.
     */
    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            log.error("Error closing file", e);
        }
    }

    // --- Accessors ---

    public boolean isTransferComplete() { return transferComplete; }
    public boolean isSendComplete() { return sendComplete; }
    public long getFileSize() { return fileSize; }
    public int getTotalChunks() { return totalChunks; }

    // --- Inner class ---

    private static class PendingChunk {
        final int chunkIndex;
        final long offset;
        final byte[] data;
        final boolean isLastChunk;

        PendingChunk(int chunkIndex, long offset, byte[] data, boolean isLastChunk) {
            this.chunkIndex = chunkIndex;
            this.offset = offset;
            this.data = data;
            this.isLastChunk = isLastChunk;
        }
    }
}
