package com.nettyfile.transfer.client;

import com.nettyfile.transfer.protocol.FileTransferPacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;


public class ClientChannelHandler extends SimpleChannelInboundHandler<FileTransferPacket> {

    private static final Logger log = LoggerFactory.getLogger(ClientChannelHandler.class);

    private enum State { AWAITING_HANDSHAKE_RESPONSE, SENDING, COMPLETE }
    private State state = State.AWAITING_HANDSHAKE_RESPONSE;

    private final Path filePath;

    private final long fileId;
    private final RateLimiter rateLimiter;
    private FileSender fileSender;

    /**
     * @param filePath    path to the file to send
     * @param fileId      unique identifier for this file transfer
     * @param rateLimiter rate limiter (null for no rate limiting)
     */
    public ClientChannelHandler(Path filePath, long fileId, RateLimiter rateLimiter) {
        this.filePath = filePath;
        this.fileId = fileId;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 一旦通道连接成功，立即发送握手信号
        String fileName = filePath.getFileName().toString();
        long fileSize = filePath.toFile().length();
        FileTransferPacket handshake = FileTransferPacket.handshake(fileId, fileName, fileSize);
        ctx.writeAndFlush(handshake);
        log.info("Sent HANDSHAKE: fileId={}, name='{}', size={} bytes ({} MB)",
                fileId, fileName, fileSize, fileSize / (1024 * 1024));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FileTransferPacket packet) {
        switch (packet.getPacketType()) {
            case HANDSHAKE_RESPONSE:
                handleHandshakeResponse(ctx, packet);
                break;
            case ACK:
                handleAck(ctx, packet);
                break;
            case NACK:
                handleNack(ctx, packet);
                break;
            case COMPLETE:
                handleComplete(ctx, packet);
                break;
            default:
                log.warn("Unexpected packet type on client: {}", packet.getPacketType());
        }
    }

    // ========================================================================
    // HANDSHAKE RESPONSE
    // ========================================================================

    private void handleHandshakeResponse(ChannelHandlerContext ctx, FileTransferPacket packet) {
        if (state != State.AWAITING_HANDSHAKE_RESPONSE) {
            log.warn("Unexpected HANDSHAKE_RESPONSE in state {}", state);
            return;
        }

        long resumeOffset = packet.getOffset();
        long totalFileSize = packet.getNextExpectedOffset();

        log.info("Received HANDSHAKE_RESPONSE: resumeOffset={}, totalSize={}",
                resumeOffset, totalFileSize);

        // 创建文件发送器
        try {
            fileSender = new FileSender(ctx.channel(), filePath, fileId, resumeOffset, rateLimiter);
        } catch (Exception e) {
            log.error("Failed to create FileSender: {}", e.getMessage(), e);
            ctx.close();
            return;
        }

        // 为流量控制注册通道可写性监听器
        // 当通道在背压后再次变得可写时，恢复发送
        ctx.channel().closeFuture().addListener(future -> {
            if (fileSender != null && !fileSender.isTransferComplete()) {
                log.warn("Channel closed before transfer completed");
                fileSender.close();
            }
        });

        state = State.SENDING;

        // Start sending chunks
        fileSender.start();
    }

    // ========================================================================
    // ACK / NACK
    // ========================================================================

    private void handleAck(ChannelHandlerContext ctx, FileTransferPacket packet) {
        if (fileSender == null) return;

        long receivedOffset = packet.getOffset();
        fileSender.onAck(receivedOffset);

        // 检查是否完成传完了
        if (fileSender.isTransferComplete()) {
            state = State.COMPLETE;
            fileSender.close();
            log.info("Transfer complete. Closing connection.");
            ctx.close();
        }
    }

    private void handleNack(ChannelHandlerContext ctx, FileTransferPacket packet) {
        if (fileSender == null) return;

        int missingChunkIndex = packet.getChunkIndex();
        fileSender.onNack(missingChunkIndex);
    }

    // ========================================================================
    // COMPLETE
    // ========================================================================
    private void handleComplete(ChannelHandlerContext ctx, FileTransferPacket packet) {
        log.info("Received COMPLETE from server. Transfer finished successfully.");
        state = State.COMPLETE;
        if (fileSender != null) {
            fileSender.close();
        }
        ctx.close();
    }

    // ========================================================================
    // FLOW CONTROL: 通道可写性
    // ========================================================================

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        // Called by Netty when the channel's writability changes
        // (i.e., when the write buffer crosses the high/low watermark)
        if (ctx.channel().isWritable() && fileSender != null) {
            log.debug("Channel became writable — resuming send");
            // We need to resume on the event loop, not from this callback thread.
            // Actually, channelWritabilityChanged IS called on the event loop,
            // so it's safe to call directly.
            fileSender.onChannelWritable();
        }
    }

    // ========================================================================
    // LIFECYCLE
    // ========================================================================

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("Channel disconnected");
        if (fileSender != null && !fileSender.isTransferComplete()) {
            log.warn("Transfer interrupted — progress may have been saved on server for resume");
            fileSender.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Client channel error: {}", cause.getMessage(), cause);
        if (fileSender != null) {
            fileSender.close();
        }
        ctx.close();
    }
}
