package com.nettyfile.transfer.nack;

import com.nettyfile.transfer.protocol.FileTransferCodec;
import com.nettyfile.transfer.protocol.FileTransferPacket;
import com.nettyfile.transfer.protocol.PacketType;
import com.nettyfile.transfer.util.MD5Util;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Random;

/**
 * 最小化NACK测试客户端。
 * <p>
 * 该客户端故意跳过某些数据块（chunk）编号，制造接收间隙，
 * 从而触发服务端发送NACK请求重传。用于验证NACK/重传机制的端到端行为。
 * </p>
 *
 * <h3>用法</h3>
 * <pre>
 * java com.nettyfile.transfer.nack.NackTestClient &lt;host&gt; &lt;port&gt;
 * 例如:
 * java com.nettyfile.transfer.nack.NackTestClient localhost 8899
 * </pre>
 *
 * <h3>工作流程</h3>
 * <ol>
 *   <li>连接服务端并发送握手包</li>
 *   <li>等待握手响应</li>
 *   <li>发送数据块，但故意跳过 chunk 1 和 chunk 3（可配置）</li>
 *   <li>监听服务端发来的 NACK 包</li>
 *   <li>收到 NACK 后立即重传缺失的 chunk</li>
 *   <li>打印清晰的日志以展示 NACK 交互过程</li>
 * </ol>
 */
public class NackTestClient {


    /** 数据块大小: 1KB (测试用，不需要1MB) */
    private static final int CHUNK_SIZE = 1024;


    /** 总数据块数 */
    private static final int TOTAL_CHUNKS = 10;

    /** 服务端 NACK 超时时间估算 */
    private static final int NACK_WAIT_MS = 3000;

    /** 文件ID（固定测试值） */
    private static final long FILE_ID = 9999L;

    /** 测试文件名 */
    private static final String FILE_NAME = "nack_test_file.bin";

    public static void main(String[] args) throws Exception {
        String host = args.length >= 1 ? args[0] : "localhost";
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 8787;

        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║       NACK 测试客户端启动               ║");
        System.out.println("║  目标: " + String.format("%-30s", host + ":" + port) + "║");
        System.out.println("║  策略: 故意跳过 chunk 1、3 制造间隙     ║");
        System.out.println("╚══════════════════════════════════════════╝");

        // 生成测试数据（所有 chunk 数据预先计算好）
        byte[][] chunks = generateTestChunks(TOTAL_CHUNKS, CHUNK_SIZE);

        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            NackTestHandler handler = new NackTestHandler(chunks);
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 使用项目已有的编解码器
                            pipeline.addLast(new FileTransferCodec.Encoder());
                            pipeline.addLast(new FileTransferCodec.Decoder());
                            pipeline.addLast(handler);
                        }
                    });

            ChannelFuture future = bootstrap.connect(host, port).sync();
            System.out.println("[客户端] 连接成功: " + future.channel().localAddress());

            // 等待通道关闭
            future.channel().closeFuture().sync();

        } finally {
            group.shutdownGracefully();
            System.out.println("[客户端] 退出");
        }
    }

    /**
     * 生成指定大小的测试数据块，数据内容为递增字节便于验证。
     */
    private static byte[][] generateTestChunks(int totalChunks, int chunkSize) {
        byte[][] chunks = new byte[totalChunks][];
        for (int i = 0; i < totalChunks; i++) {
            byte[] data = new byte[chunkSize];
            // 填充可辨识的字节：每块以 chunkIndex 开头
            data[0] = (byte) (i & 0xFF);
            // 其余填充随机字节
            new Random(i * 31).nextBytes(data);
            data[1] = (byte) ((i >> 8) & 0xFF);
            chunks[i] = data;
        }
        System.out.println("[客户端] 已生成 " + totalChunks + " 个测试数据块 (每块 " + chunkSize + " 字节)");
        return chunks;
    }

    /**
     * Netty 通道处理器：负责握手、发送带间隙的数据、处理 NACK/ACK。
     */
    private static class NackTestHandler extends SimpleChannelInboundHandler<FileTransferPacket> {

        /** 预先生成的所有数据块 */
        private final byte[][] allChunks;

        /** 故意跳过的 chunk 索引列表（逗号分隔配置） */
        private static final int[] SKIP_CHUNKS = {1, 3};

        /** 当前状态 */
        private enum State { HANDSHAKE_SENT, AWAITING_RESPONSE, SENDING, WAIT_NACK, COMPLETE }
        private State state = State.HANDSHAKE_SENT;

        /** 已发送的最后一个连续 chunk 索引 */
        private int lastSentIndex = -1;

        /** 记录已收到的 NACK 次数 */
        private int nackCount = 0;

        /** 记录已收到的 ACK 次数 */
        private int ackCount = 0;

        NackTestHandler(byte[][] allChunks) {
            this.allChunks = allChunks;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // 发送握手包
            long totalSize = (long) allChunks.length * CHUNK_SIZE * 1024;
            FileTransferPacket handshake = FileTransferPacket.handshake(FILE_ID, FILE_NAME, totalSize);
            ctx.writeAndFlush(handshake);
            state = State.AWAITING_RESPONSE;
            System.out.println("[客户端] >>> 发送 HANDSHAKE: fileId=" + FILE_ID
                    + ", name=" + FILE_NAME + ", size=" + totalSize + " bytes");
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FileTransferPacket packet) {
            System.out.println("[客户端] <<< 收到: " + packet);

            switch (packet.getPacketType()) {
                case HANDSHAKE_RESPONSE:
                    onHandshakeResponse(ctx, packet);
                    break;
                case NACK:
                    onNack(ctx, packet);
                    break;
                case ACK:
                    onAck(ctx, packet);
                    break;
                case COMPLETE:
                    onComplete(ctx, packet);
                    break;
                default:
                    System.out.println("[客户端] 未预期的包类型: " + packet.getPacketType());
            }
        }

        // ================================================================
        // 握手响应：开始发送数据（故意跳过某些 chunk）
        // ================================================================
        private void onHandshakeResponse(ChannelHandlerContext ctx, FileTransferPacket packet) {
            long resumeOffset = packet.getOffset();
            System.out.println("[客户端] 握手响应: resumeOffset=" + resumeOffset);

            state = State.SENDING;

            System.out.println("\n══════════════════════════════════════════");
            System.out.println("  开始发送数据块（策略: 跳过 chunk " + skipsToString() + "）");
            System.out.println("══════════════════════════════════════════\n");

            // 逐个发送 chunk，但跳过 SKIP_CHUNKS 中的索引
            for (int i = 0; i < allChunks.length; i++) {
                if (shouldSkip(i)) {
                    System.out.println("  ⏭  跳过 chunk " + i + " — 故意制造间隙！");
                    continue; // <<< 核心：故意不发送这个 chunk
                }
                //让它睡眠(用来调节的)
//                if ( i == 5) {
//                    try {
//                        Thread.sleep(10000);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
                sendChunk(ctx, i, i == allChunks.length - 1);
                lastSentIndex = i;
            }

            System.out.println("\n══════════════════════════════════════════");
            System.out.println("  首批数据发送完毕。已发送块索引: 0~"
                    + allChunks.length + " (跳过 " + skipsToString() + ")");
            System.out.println("  等待服务端 NACK... (最长 " + NACK_WAIT_MS + "ms)");
            System.out.println("══════════════════════════════════════════\n");

            state = State.WAIT_NACK;

            // 设置超时：如果NACK迟迟不到，手动补发缺失的chunk
            ctx.executor().schedule(() -> {
                if (state == State.WAIT_NACK && nackCount < SKIP_CHUNKS.length) {
                    System.out.println("\n⏰ 超时 " + NACK_WAIT_MS + "ms 未收到足够的 NACK！");
                    System.out.println("  收到 NACK 次数: " + nackCount + "，预期: " + SKIP_CHUNKS.length);
                    System.out.println("  手动补发缺失的 chunk...\n");
                    resendMissingChunks(ctx);
                }
            }, NACK_WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        // ================================================================
        // NACK 处理：收到重传请求
        // ================================================================

        private void onNack(ChannelHandlerContext ctx, FileTransferPacket packet) {
            nackCount++;
            int missingChunk = packet.getChunkIndex();

            System.out.println("\n  🔔 收到 NACK #" + nackCount
                    + ": 服务端请求重传 chunk " + missingChunk);
            System.out.println("     NACK 包详情: chunkIndex=" + missingChunk
                    + ", offset=" + packet.getOffset());

            if (missingChunk < 0 || missingChunk >= allChunks.length) {
                System.out.println("  ⚠ 无效的 chunk 索引 " + missingChunk + "，忽略");
                return;
            }

            // 重传缺失的 chunk（带 RETRANSMIT 标记）
            FileTransferPacket retxPacket = FileTransferPacket.retransmit(
                    FILE_ID, FILE_NAME,
                    missingChunk,
                    (long) missingChunk * CHUNK_SIZE,
                    allChunks[missingChunk],
                    missingChunk == allChunks.length - 1);

            ctx.writeAndFlush(retxPacket).addListener(f -> {
                if (f.isSuccess()) {
                    System.out.println("  ✅ 已重传 chunk " + missingChunk + " (RETRANSMIT)");
                } else {
                    System.out.println("  ❌ 重传 chunk " + missingChunk + " 失败: " + f.cause());
                }
            });

            // 如果所有跳过的chunk都已收到NACK，说明交互正常
            if (nackCount >= SKIP_CHUNKS.length) {
                System.out.println("\n══════════════════════════════════════════");
                System.out.println("  ✅ NACK 测试成功！");
                System.out.println("  共收到 " + nackCount + " 次 NACK，已重传所有缺失 chunk");
                System.out.println("══════════════════════════════════════════\n");
            }
        }

        // ================================================================
        // ACK 处理
        // ================================================================

        private void onAck(ChannelHandlerContext ctx, FileTransferPacket packet) {
            ackCount++;
            long receivedOffset = packet.getOffset();
            long nextExpected = packet.getNextExpectedOffset();

            System.out.println("  ✔ 收到 ACK #" + ackCount
                    + ": received=" + receivedOffset
                    + ", nextExpected=" + nextExpected
                    + " (" + String.format("%.1f", receivedOffset * 100.0 / (allChunks.length * CHUNK_SIZE)) + "%)");

            if (receivedOffset >= allChunks.length * CHUNK_SIZE) {
                System.out.println("\n══════════════════════════════════════════");
                System.out.println("  🎉 传输完成！所有 chunk 已确认。");
                System.out.println("  统计: ACK=" + ackCount + ", NACK=" + nackCount);
                System.out.println("══════════════════════════════════════════\n");
                state = State.COMPLETE;
                ctx.close();
            }
        }

        // ================================================================
        // COMPLETE 处理
        // ================================================================

        private void onComplete(ChannelHandlerContext ctx, FileTransferPacket packet) {
            System.out.println("\n  🏁 服务端发送 COMPLETE 通知。传输结束。\n");
            state = State.COMPLETE;
            ctx.close();
        }

        // ================================================================
        // 辅助方法
        // ================================================================

        /** 发送单个 chunk */
        private void sendChunk(ChannelHandlerContext ctx, int index, boolean isLast) {
            long offset = (long) index * CHUNK_SIZE;
            FileTransferPacket packet = FileTransferPacket.data(
                    FILE_ID, FILE_NAME,
                    index, offset,
                    allChunks[index], isLast);

            ctx.writeAndFlush(packet).addListener(f -> {
                if (f.isSuccess()) {
                    System.out.println("  ➤ 发送 chunk " + index
                            + " (offset=" + offset + ", size=" + allChunks[index].length
                            + (isLast ? ", LAST" : "") + ")");
                } else {
                    System.out.println("  ❌ 发送 chunk " + index + " 失败: " + f.cause());
                }
            });
        }

        /** 判断该 chunk 是否应该跳过 */
        private boolean shouldSkip(int chunkIndex) {
            for (int skip : SKIP_CHUNKS) {
                if (chunkIndex == skip) return true;
            }
            return false;
        }

        /** 超时兜底：手动补发所有缺失的 chunk */
        private void resendMissingChunks(ChannelHandlerContext ctx) {
            for (int skip : SKIP_CHUNKS) {
                System.out.println("  ↻ 手动补发 chunk " + skip);
                sendChunk(ctx, skip, skip == allChunks.length - 1);
            }
        }

        /** 可读的跳过列表 */
        private static String skipsToString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < SKIP_CHUNKS.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(SKIP_CHUNKS[i]);
            }
            return sb.toString();
        }

        // ================================================================
        // 生命周期
        // ================================================================

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("[客户端] 连接断开");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[客户端] 异常: " + cause.getMessage());
            cause.printStackTrace();
            ctx.close();
        }
    }
}
