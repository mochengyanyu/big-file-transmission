package com.nettyfile.transfer.server;

import com.nettyfile.transfer.protocol.FileTransferPacket;
import com.nettyfile.transfer.protocol.PacketType;
import com.nettyfile.transfer.util.MD5Util;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


public class ServerChannelHandler extends SimpleChannelInboundHandler<FileTransferPacket> {

    private static final Logger log = LoggerFactory.getLogger(ServerChannelHandler.class);


    private static final long NACK_TIMEOUT_MS = 100;

    static final int CHUNK_SIZE = 1024 * 1024; // 1MB

    private enum State { AWAITING_HANDSHAKE, TRANSFERRING, COMPLETE }
    private State state = State.AWAITING_HANDSHAKE;

    private final Path outputDir;
    private FileReceiver fileReceiver;
    private ReceiveBuffer receiveBuffer;
    private long fileId;
    private String fileName;
    private long totalFileSize;

    // ***NACK定时器***
    private ScheduledFuture<?> nackFuture;

    public ServerChannelHandler(Path outputDir) {
        this.outputDir = outputDir;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FileTransferPacket packet) {
        switch (packet.getPacketType()) {
            case HANDSHAKE:
                handleHandshake(ctx, packet);
                break;
            case DATA:
                handleData(ctx, packet);
                break;
            case COMPLETE:
                handleComplete(ctx, packet);
                break;
            default:
                log.warn("Unexpected packet type on server: {}", packet.getPacketType());
        }
    }

    // ========================================================================
    // HANDSHAKE
    // ========================================================================
    private void handleHandshake(ChannelHandlerContext ctx, FileTransferPacket packet) {
        if (state != State.AWAITING_HANDSHAKE) {
            log.warn("Received HANDSHAKE but already in state {}", state);
            return;
        }
        this.fileId = packet.getFileId();
        this.fileName = packet.getFileName();
        this.totalFileSize = packet.getNextExpectedOffset(); // carries total size in handshake
        log.info("Received HANDSHAKE: fileId={}, name='{}', size={} bytes ({} MB)",
                fileId, fileName, totalFileSize, totalFileSize / (1024 * 1024));
        // 检查当前进度（断点续传）
        long resumeOffset = FileReceiver.getResumeOffset(outputDir, fileName);
        int resumeChunkIndex = (int) (resumeOffset / CHUNK_SIZE);
        // 在恢复点初始化接收缓冲区
        this.receiveBuffer = new ReceiveBuffer(CHUNK_SIZE);
        this.receiveBuffer.setExpectedChunkIndex(resumeChunkIndex);
        // 发送带有恢复偏移量的握手响应
        FileTransferPacket response = FileTransferPacket.handshakeResponse(
                fileId, fileName, resumeOffset, totalFileSize);
        ctx.writeAndFlush(response);

        try {
            this.fileReceiver = new FileReceiver(outputDir, fileName, totalFileSize, resumeOffset);
        } catch (IOException e) {
            log.error("Failed to create FileReceiver for '{}': {}", fileName, e.getMessage());
            ctx.close();
            return;
        }

        this.state = State.TRANSFERRING;
        log.info("Handshake complete. Resume offset: {} (chunk {}). Ready for data.",
                resumeOffset, resumeChunkIndex);
    }

    // ========================================================================
    // DATA CHUNK PROCESSING
    // ========================================================================

    private void handleData(ChannelHandlerContext ctx, FileTransferPacket packet) {
        if (state != State.TRANSFERRING) {
            log.warn("Received DATA but state is {}", state);
            return;
        }
        int chunkIndex = packet.getChunkIndex();
        long offset = packet.getOffset();
        byte[] data = packet.getPayload();
        if (data == null || data.length == 0) {
            log.warn("Empty payload for chunk {}", chunkIndex);
            return;
        }
        // --- MD5 检查 ---
        byte[] expectedMd5 = packet.getMd5();
        byte[] actualMd5 = MD5Util.compute(data);
        if (!MD5Util.equals(expectedMd5, actualMd5)) {
            log.warn("MD5 mismatch for chunk {} (offset {}). Expected: {}, Actual: {}. " +
                            "Chunk will be discarded and retransmission requested.",
                    chunkIndex, offset,
                    MD5Util.bytesToHex(expectedMd5),
                    MD5Util.bytesToHex(actualMd5));
            // 不要标记为已接收——让NACK定时器来处理
            // 但是，对于这个特定的数据块，请立即发送一个否定确认（NACK）
            sendNack(ctx, chunkIndex, offset);
            return;
        }
        // --- 提供接收缓冲区 ---
        ReceiveBuffer.OfferResult result = receiveBuffer.offer(
                chunkIndex, offset, data, packet.isLastChunk());
        switch (result.type) {
            case EXPECTED:
                handleExpectedChunk(ctx, offset, data, chunkIndex, result.isLastChunk);
                break;

            case BUFFERED:
                if (receiveBuffer.hasGap()) {
                    scheduleNack(ctx);
                }
                break;

            case DUPLICATE:
                break;
            case BUFFER_FULL:
                log.warn("Buffer full! Writing chunk {} directly to disk (overflow). " +
                         "Expected chunk {} still missing.",
                         chunkIndex, receiveBuffer.getExpectedChunkIndex());
                try {
                    fileReceiver.writeOverflow(offset, data, chunkIndex);
                } catch (IOException e) {
                    log.error("Overflow write failed for chunk {}: {}", chunkIndex, e.getMessage());
                }
                sendNack(ctx, (int) receiveBuffer.getExpectedChunkIndex(),
                        receiveBuffer.getExpectedChunkIndex() * CHUNK_SIZE);

                scheduleNack(ctx);
                break;
        }
    }

    /**
     * 将预期的数据块写入磁盘，然后刷新所有连续的缓冲数据块。
     */
    private void handleExpectedChunk(ChannelHandlerContext ctx, long offset, byte[] data,
                                      int chunkIndex, boolean isLastChunk) {
        try {
            //写期望块(这里其实可以修改,改成一个你需要规定几个一次性的落盘)
            fileReceiver.writeChunk(offset, data, chunkIndex);
            List<ReceiveBuffer.BufferedChunk> contiguous =
                    receiveBuffer.advanceAndFlushContiguous();
            for (ReceiveBuffer.BufferedChunk bc : contiguous) {
                fileReceiver.writeChunk(bc.offset, bc.data, bc.chunkIndex);
                if (bc.isLastChunk) {
                    isLastChunk = true;
                }
            }
            // 同步连续偏移量：advanceAndFlushContiguous() 跳过了溢出写入的块（已在磁盘），
            // 但这些块并未通过 writeChunk() 推进 currentOffset（溢出用的是 writeOverflow）。
            // 这里用 expectedChunkIndex × chunkSize 算出理论连续字节数，
            // 再和 totalFileSize 取最小值——最后一个块可能不足 chunkSize。
            long newContiguousOffset = receiveBuffer.getExpectedChunkIndex()
                    * (long) receiveBuffer.getChunkSize();
            fileReceiver.syncContiguousOffset(Math.min(newContiguousOffset, totalFileSize));
            // --- ACK确认：每发送10个数据块,表明我已经落盘了 ---
            if (fileReceiver.shouldSendAck()) {
                sendAck(ctx);
                fileReceiver.resetAckCounter();
                // 断点续传的进度保持
                fileReceiver.persistProgress();
            }
            // --- 检查完成 ---
            if (fileReceiver.isComplete() || isLastChunk) {
                onTransferComplete(ctx);
                return;
            }
            if (receiveBuffer.hasGap()) {
                scheduleNack(ctx);
            } else {
                cancelNack();
            }

        } catch (IOException e) {
            log.error("Disk write error for chunk {} at offset {}: {}",
                    chunkIndex, offset, e.getMessage(), e);
            ctx.close();
        }
    }

    // ========================================================================
    // ACK / NACK
    // ========================================================================

    /**
     * 发送一个包含当前已接收偏移量的确认包。
     * ACK告诉客户端：“我已收到截至该偏移量的所有字节。
     */
    private void sendAck(ChannelHandlerContext ctx) {
        long receivedOffset = fileReceiver.getCurrentOffset();
        long nextExpected = receiveBuffer.getExpectedChunkIndex() * (long) CHUNK_SIZE;

        FileTransferPacket ack = FileTransferPacket.ack(fileId, receivedOffset, nextExpected);
        ctx.writeAndFlush(ack);

        log.debug("Sent ACK: received={}, nextExpected={}, progress={}%",
                receivedOffset, nextExpected, String.format("%.1f", fileReceiver.getProgressPercent()));
    }

    /**
     * 发送一个否定确认（NACK），请求重新传输特定的缺失数据块
     */
    private void sendNack(ChannelHandlerContext ctx, int missingChunkIndex, long missingOffset) {
        FileTransferPacket nack = FileTransferPacket.nack(fileId, missingChunkIndex, missingOffset);
        ctx.writeAndFlush(nack);
        log.debug("Sent NACK for chunk {} (offset {})", missingChunkIndex, missingOffset);
    }

    /**
     * 设置一个NACK（重传的确认）定时器来检查缺失的数据块
     */
    private void scheduleNack(ChannelHandlerContext ctx) {
        if (nackFuture != null && !nackFuture.isDone()) {
            // 定时器已设置, 不做任何处理(防止出现死循环，让系统卡死)
            return;
        }

        nackFuture = ctx.executor().schedule(() -> {
            nackFuture = null;
            // 复查：自我们安排之后，这个缺口是否已经填补
            if (state != State.TRANSFERRING) {
                return;
            }
            long expectedIdx = receiveBuffer.getExpectedChunkIndex();
            if (!receiveBuffer.isReceived((int) expectedIdx)) {
                // 仍存在间隙 → 数据包可能丢失 → 请求重传
                long missingOffset = expectedIdx * CHUNK_SIZE;
                log.info("NACK timeout ({}ms): chunk {} still missing. Requesting retransmission.",
                        NACK_TIMEOUT_MS, expectedIdx);
                sendNack(ctx, (int) expectedIdx, missingOffset);
                // 重新调度：每100毫秒检查一次，直到间隙被填满
                // (这里递归调用的目的是为了防止我的Nack的包丢了)
                // 但是这里有问题的是，如果我的客户端断连了，这里会出现死循环,所以最好是搞一个递归的次数)
                scheduleNack(ctx);
            } else {
                log.debug("NACK timer: chunk {} arrived in time, no retransmission needed.",
                        expectedIdx);
            }
        }, NACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 取消当前正在运行的NACK（否定确认）定时器。
     */
    private void cancelNack() {
        if (nackFuture != null) {
            nackFuture.cancel(false);
            nackFuture = null;
        }
    }

    private void handleComplete(ChannelHandlerContext ctx, FileTransferPacket packet) {
        log.info("Received COMPLETE notification from client for fileId={}", packet.getFileId());
        // 客户端认为传输已完成 — 发送最终确认并关闭
        if (fileReceiver != null) {
            sendAck(ctx);
            fileReceiver.close();
        }
        this.state = State.COMPLETE;
        ctx.close();
    }

    private void onTransferComplete(ChannelHandlerContext ctx) {
        log.info("Transfer complete for '{}': {} / {} bytes (100%)",
                fileName, fileReceiver.getCurrentOffset(), totalFileSize);

        // Send final ACK
        sendAck(ctx);
        FileTransferPacket complete = FileTransferPacket.complete(fileId, totalFileSize);
        ctx.writeAndFlush(complete);
        // 清除
        if (fileReceiver != null) {
            fileReceiver.close();
        }
        this.state = State.COMPLETE;
        cancelNack();
        log.info("File '{}' transfer finished successfully.", fileName);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 连接已关闭 — 请保存进度以便后续恢复
        cancelNack();
        if (fileReceiver != null && state == State.TRANSFERRING) {
            try {
                fileReceiver.persistProgress();
            } catch (IOException e) {
                log.error("Failed to persist progress on disconnect", e);
            }
            fileReceiver.closeForResume();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel exception: {}", cause.getMessage(), cause);
        cancelNack();
        if (fileReceiver != null && state == State.TRANSFERRING) {
            try {
                fileReceiver.persistProgress();
            } catch (IOException e) {
                log.error("Failed to persist progress on error", e);
            }
            fileReceiver.closeForResume();
        }
        ctx.close();
    }
}
