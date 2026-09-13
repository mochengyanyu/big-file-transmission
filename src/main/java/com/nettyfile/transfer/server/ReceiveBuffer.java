package com.nettyfile.transfer.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class ReceiveBuffer {

    private static final Logger log = LoggerFactory.getLogger(ReceiveBuffer.class);

    /**
     * 缓冲区容量
     */
    public static final int BUFFER_CAPACITY = 10;


    public static class BufferedChunk {
        public final int chunkIndex;
        public final long offset;
        public final byte[] data;
        public final boolean isLastChunk;

        BufferedChunk(int chunkIndex, long offset, byte[] data, boolean isLastChunk) {
            this.chunkIndex = chunkIndex;
            this.offset = offset;
            this.data = data;
            this.isLastChunk = isLastChunk;
        }
    }


    private final BufferedChunk[] buffer = new BufferedChunk[BUFFER_CAPACITY];


    private final BitSet receivedChunks = new BitSet();

    /**
     * 下标的映射关系
     */
    private final Map<Integer, Integer> chunkIndexToSlot = new HashMap<>();


    private final Deque<Integer> freeSlots = new ArrayDeque<>();

    /** 下一块的期待值（落盘的下一块的期待值） */
    private long expectedChunkIndex = 0;


    private final int chunkSize;

    /**
     * @param chunkSize fixed chunk size in bytes (e.g. 1048576 = 1MB)
     */
    public ReceiveBuffer(int chunkSize) {
        this.chunkSize = chunkSize;
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            freeSlots.offer(i);
        }
    }

    /**
     * 将新到达的数据块提供给缓冲区
     *
     * @param chunkIndex the index of this chunk in the file
     * @param offset     byte offset of this chunk in the file
     * @param data       raw chunk payload
     * @param isLast     whether this is the final chunk of the file
     * @return {@link OfferResult} telling the caller what to do next
     */
    public OfferResult offer(int chunkIndex, long offset, byte[] data, boolean isLast) {
        // --- 幂等性门控 ---
        // 如果我们已经见过这个数据块（重复的数据包或重新传输的数据包），
        // 悄悄地丢弃它。
        if (receivedChunks.get(chunkIndex)) {
            log.debug("Duplicate chunk {}, ignoring (idempotent)", chunkIndex);
            return OfferResult.DUPLICATE;
        }

        // 在位图中立即标记为已接收 — 这可以防止在我们完成处理之前，
        // 相同的块再次到达时发生竞争。
        receivedChunks.set(chunkIndex);

        if (chunkIndex == expectedChunkIndex) {
            // 这正是所期待的部分。
            // 告诉调用者直接写入即可，无需缓冲。
            log.debug("Chunk {} is expected next, write-through", chunkIndex);
            return new OfferResult(OfferResult.Type.EXPECTED, null, isLast);
        }

        // --- 无序块：暂存 ---
        Integer freeSlot = freeSlots.poll();
        if (freeSlot == null) {
            // 缓冲区已满。保持 BitSet 标记（数据已接收） —
            // 调用方会将此块作为溢出直接写入磁盘。
            // 在正确的流量控制下，这种情况应很少发生。
            // 当间隙最终被填满时，advanceAndFlushContiguous()
            // 看到 BitSet 位并跳过此数据块（它已在磁盘上）。
            log.warn("Receive buffer full ({} slots). Expected chunk {} not arrived. " +
                            "Writing chunk {} directly to disk (overflow).",
                    BUFFER_CAPACITY, expectedChunkIndex, chunkIndex);
            // BitSet保持不变——请勿清除
            BufferedChunk bc = new BufferedChunk(chunkIndex, offset, data, isLast);
            return new OfferResult(OfferResult.Type.BUFFER_FULL, bc, isLast);
        }

        // 存储到缓冲区中
        BufferedChunk bc = new BufferedChunk(chunkIndex, offset, data, isLast);
        buffer[freeSlot] = bc;
        chunkIndexToSlot.put(chunkIndex, freeSlot);

        log.debug("Buffered chunk {} at slot {} (buffered: {}, free: {})",
                chunkIndex, freeSlot, chunkIndexToSlot.size(), freeSlots.size());

        return new OfferResult(OfferResult.Type.BUFFERED, bc, false);
    }

    /**
     * 在将预期的数据块写入磁盘后，推进预期指针*，并收集所有现在已准备好的连续缓冲数据块。
     * <p>这是关键的“刷新连续”操作：
     * 如果缓冲区包含块5、6、7，并且写入器刚完成块4的写入，
     * 此操作将按顺序返回块5、6、7，以便它们可以一起批量写入磁盘。
     * </p> * * @return 按索引顺序排列的准备进行磁盘写入的连续块列表（可能为空）
     */
    public List<BufferedChunk> advanceAndFlushContiguous() {
        expectedChunkIndex++;
        List<BufferedChunk> ready = new ArrayList<>();

        while (receivedChunks.get((int) expectedChunkIndex)) {
            Integer slot = chunkIndexToSlot.remove((int) expectedChunkIndex);

            if (slot != null) {
                // Chunk was buffered — retrieve it and free the slot
                BufferedChunk bc = buffer[slot];
                buffer[slot] = null;
                freeSlots.offer(slot);
                ready.add(bc);
            }

            expectedChunkIndex++;
        }

        if (!ready.isEmpty()) {
            log.debug("Flushed {} contiguous buffered chunks, next expected: {}",
                    ready.size(), expectedChunkIndex);
        }

        return ready;
    }

    /**
     * 如果存在间隔（即预期的数据块缺失，但后续数据块已到达），则返回true。这会触发NACK定时器。
     */
    public boolean hasGap() {
        if (receivedChunks.get((int) expectedChunkIndex)) {
            return false; // expected chunk is here, no gap
        }
        // 检查是否存在后续的数据块
        int nextSet = receivedChunks.nextSetBit((int) expectedChunkIndex + 1);
        return nextSet != -1;
    }

    /**
     * 检查是否已收到特定数据块
     */
    public boolean isReceived(int chunkIndex) {
        return receivedChunks.get(chunkIndex);
    }

    // --- Accessors ---

    public long getExpectedChunkIndex() {
        return expectedChunkIndex;
    }

    /** Set the expected chunk index (used when resuming a transfer). */
    public void setExpectedChunkIndex(long index) {
        this.expectedChunkIndex = index;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getBufferedCount() {
        return chunkIndexToSlot.size();
    }

    public boolean isFull() {
        return freeSlots.isEmpty();
    }

    public int freeSlotCount() {
        return freeSlots.size();
    }

    // --- Result type ---

    /**
     * Result of offering a chunk to the buffer.
     */
    public static class OfferResult {

        public enum Type {
            /** 这是预期的数据块——调用者应立即将其写入磁盘。 */
            EXPECTED,
            /** 数据块被存储在缓冲区中，以供后续检索 */
            BUFFERED,
            /** 数据块已收到（幂等丢弃）。 */
            DUPLICATE,
            /** 缓冲区已满 — 数据块被丢弃。流量控制应能防止这种情况发生。 */
            BUFFER_FULL
        }

        public final Type type;
        public final BufferedChunk bufferedChunk;
        public final boolean isLastChunk;

        OfferResult(Type type, BufferedChunk bufferedChunk, boolean isLastChunk) {
            this.type = type;
            this.bufferedChunk = bufferedChunk;
            this.isLastChunk = isLastChunk;
        }

        // Singleton instance for duplicate (the one result that never carries data)
        private static final OfferResult DUPLICATE = new OfferResult(Type.DUPLICATE, null, false);

        public static OfferResult duplicate() { return DUPLICATE; }
    }
}
