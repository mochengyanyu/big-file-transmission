package com.nettyfile.transfer.server;

import com.nettyfile.transfer.protocol.FileTransferCodec;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 硬件是4核8线程16G内存，100M的专线宽带
 */
public class FileTransferServer {


    private static final Logger log = LoggerFactory.getLogger(FileTransferServer.class);

    // ========================================================================
    //  Flow control parameters — derivation chain:
    //
    //  ① BDP = bandwidth × RTT = 10 MB/s × 50 ms = 512 KB
    //     → 网络管道中最多同时容纳 512KB 数据
    //
    //  ② TCP receive buffer = 600 KB
    //     → 内核套接字缓冲区，略大于 BDP 以吸收抖动
    //     → 内核通过 TCP window 通告发送端：接收窗口 = 600KB
    //
    //  ③ Netty high watermark = 512 KB (= BDP, ≤ TCP buffer)
    //     → 当 Netty 应用层写缓冲区积压达到一个 BDP 时，
    //        channel.isWritable() 返回 false，触发应用层反压
    //     → 为什么不是 600KB？因为高水位到达时内核缓冲可能还未排空，
    //        取 BDP 值给了内核一个缓冲窗口，避免反压信号延迟
    //
    //  ④ Netty low watermark = 256 KB (= BDP / 2)
    //     → 写缓冲区降到低水位以下时，isWritable() 恢复为 true
    //     → 高低水位之间作为滞回区间，避免频繁的暂停/恢复切换
    // ========================================================================

    /** TCP 接收缓冲区: 600 KB（内核层面流控） */
    private static final int TCP_RECV_BUFFER = 600 * 1024;
    /** TCP 发送缓冲区: 600 KB */
    private static final int TCP_SEND_BUFFER = 600 * 1024;

    /** Netty 写缓冲区高水位: 4 MB（我第一次是和tcp挂钩了,
     * 以至于每一次一个数据包就把水位线打满了，
     * 反而是增加事件频繁的切换,
     * 其实这里不应该和tcp的缓存区挂钩，
     * 这里只是为了在你应用层做的控制。所以我是扩大到了4MB） */
    private static final int HIGH_WATERMARK = 4 * 1024 * 1024;
    /** Netty 写缓冲区低水位: 2 M（低水位我扩大到了2MB） */
    private static final int LOW_WATERMARK = 2 * 1024 *1024;

    // ========================================================================
    //  Traffic shaping — 服务端限速：
    //  将速率限制放在服务端，利用 TCP 反压自然传导到客户端。
    //  ChannelTrafficShapingHandler 限制 Netty 从 socket 读取的速度，
    //  → 内核接收缓冲区积满 → TCP 窗口缩小 → 客户端发送缓冲区积满
    //  → 客户端 write() 阻塞 → channel.isWritable()=false → 暂停
    //  相比客户端令牌桶的优势：
    //  ① Netty 内置，不用手写限速代码
    //  ② 反压通过 TCP 自动传导，无需应用层协调
    //  ③ 配置统一在服务端，客户端无需感知限速策略
    // ========================================================================

    /** 入站读取限速: 10 MB/s（对应 80 Mbps 带宽预留） */
    private static final long INBOUND_READ_LIMIT = 10 * 1024 * 1024; // 10 MB/s
    /** 出站写入不限速（ACK/NACK 包都很小） */
    private static final long OUTBOUND_WRITE_LIMIT = 0; // 0 = unlimited
    /** 流量整形的检查间隔: 1 秒 */
    private static final long TRAFFIC_CHECK_INTERVAL = 1000; // ms

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final Path outputDir;

    /**
     * @param outputDir 用于存储接收文件的目录
     */
    public FileTransferServer(Path outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * @param outputPath 输出目录的路径
     */
    public FileTransferServer(String outputPath) {
        this(Paths.get(outputPath));
    }

    /**
     * 服务启动流程
     * @param port TCP port to bind
     * @throws InterruptedException if the bind is interrupted
     */
    public void start(int port) throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        //这里不想去调了，就用它默认的cpu核数
        // int workerThreads = Math.min(Runtime.getRuntime().availableProcessors(), 8);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_RCVBUF, TCP_RECV_BUFFER)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_RCVBUF, TCP_RECV_BUFFER)
                .childOption(ChannelOption.SO_SNDBUF, TCP_SEND_BUFFER)
                // Netty watermarks for flow control
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(LOW_WATERMARK, HIGH_WATERMARK))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // --- Pipeline (bottom → top) ---
                        // ① TrafficShaping: 限制入站读取速率 10MB/s
                        //    放在 pipeline 最前面，在解码之前就控制 socket 的读取速度
                        //    服务端读得慢 → 内核 recv buffer 满 → TCP 窗口缩小
                        //    → 客户端内核 send buffer 满 → write() 阻塞 → 自动反压
                        pipeline.addLast("trafficShaper",
                                new ChannelTrafficShapingHandler(
                                        OUTBOUND_WRITE_LIMIT,   // 出站不限速
                                        INBOUND_READ_LIMIT,      // 入站 10MB/s
                                        TRAFFIC_CHECK_INTERVAL));
                        //其实这里我应该添加心跳的机制的(但是我没做了)
                        // ② Decoder: bytes → FileTransferPacket
                        pipeline.addLast("decoder", new FileTransferCodec.Decoder());
                        // ③ Encoder: FileTransferPacket → bytes
                        pipeline.addLast("encoder", new FileTransferCodec.Encoder());
                        // ④ 实际业务的处理
                        pipeline.addLast("handler", new ServerChannelHandler(outputDir));
                        log.debug("New channel initialized: {}", ch.remoteAddress());
                    }
                });

        // (同步的方式,不差那一点时间)
        ChannelFuture future = bootstrap.bind(port).syncUninterruptibly();
        if (future.isSuccess()) {
            serverChannel = future.channel();
            log.info("FileTransferServer started on port {} (output dir: {})", port, outputDir);
            log.info("TCP recv buffer: {}KB, Watermarks: high={}KB low={}KB, TrafficShaping: inbound={}MB/s",
                    TCP_RECV_BUFFER / 1024, HIGH_WATERMARK / 1024, LOW_WATERMARK / 1024,
                    INBOUND_READ_LIMIT / (1024 * 1024));
        } else {
            log.error("Failed to bind to port {}", port);
            throw new RuntimeException("Failed to bind to port " + port, future.cause());
        }
    }

    /**
     * 启动服务器并阻塞，直到服务器通道关闭。
     */
    public void startAndWait(int port) throws InterruptedException {
        start(port);
        serverChannel.closeFuture().sync();
    }

    /**
     * 优雅地停止服务器。清空资源
     */
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        log.info("FileTransferServer stopped.");
    }

    public Path getOutputDir() {
        return outputDir;
    }

    public static void main(String[] args) throws Exception {
        int port = 8787;
        String outputPath = "C:/Users/14827/Desktop/";

        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            outputPath = args[1];
        }

        FileTransferServer server = new FileTransferServer(outputPath);
        // 添加关机钩子以实现优雅停止
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        server.startAndWait(port);
    }
}
