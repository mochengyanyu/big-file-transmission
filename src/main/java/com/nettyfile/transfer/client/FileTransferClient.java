package com.nettyfile.transfer.client;

import com.nettyfile.transfer.protocol.FileTransferCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**  * 基于Netty的文件传输客户端。
 * * <h3>配置</h3>
 * * <ul> *
 * <li><b>TCP发送/接收缓冲区</b>：600 KB — 与BDP匹配。</li> *
 * <li><b>TCP_NODELAY</b>：启用（无Nagle）。</li>
 * *<li><b>SO_KEEPALIVE</b>：启用。</li>
 * * <li><b>速率限制</b>：10 MB/s令牌桶（匹配80 Mbps）。</li>
 * * </ul>
 * * * <h3>使用</h3>
 * * <pre>
 *     * FileTransferClient client = new FileTransferClient();
 *     * client.connect("server-host", 8080, Paths.get("/data/bigfile.iso"));
 *     * client.waitForCompletion();
 *     * client.stop();
 *     *
 * </pre> *
 * */
public class FileTransferClient {


    private static final Logger log = LoggerFactory.getLogger(FileTransferClient.class);

    // ========================================================================
    //  Flow control — derivation chain (see FileTransferServer for details):
    //  BDP → TCP buffer → Netty watermarks
    // ========================================================================

    /** TCP 缓冲区: 600 KB（BDP = 512KB，上取整留余量） */
    private static final int TCP_BUFFER_SIZE = 600 * 1024;

    /** Netty 写缓冲区高水位: 512 KB（= BDP，≦ TCP 缓冲区） */
    private static final int HIGH_WATERMARK = 512 * 1024;

    /** Netty 写缓冲区低水位: 256 KB（= BDP / 2，滞回区间） */
    private static final int LOW_WATERMARK = 256 * 1024;

    private EventLoopGroup group;
    private Channel channel;
    private final RateLimiter rateLimiter;

    public FileTransferClient() {
        this.rateLimiter = new RateLimiter();
    }

    /**
     * @param rateLimiter custom rate limiter (null to disable rate limiting)
     */
    public FileTransferClient(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    /**
     * Connect to the server and start a file transfer.
     *
     * @param host     server hostname or IP
     * @param port     server port
     * @param filePath 要发送的文件的路径
     * @return 当连接建立时完成的 ChannelFuture
     * @throws InterruptedException 如果连接中断
     */
    public ChannelFuture connect(String host, int port, Path filePath) throws InterruptedException {
        if (!filePath.toFile().exists()) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }
        if (!filePath.toFile().isFile()) {
            throw new IllegalArgumentException("Not a regular file: " + filePath);
        }

        long fileId = generateFileId(filePath);
        group = new NioEventLoopGroup(1); // Single thread is enough for one client

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_RCVBUF, TCP_BUFFER_SIZE)
                .option(ChannelOption.SO_SNDBUF, TCP_BUFFER_SIZE)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(LOW_WATERMARK, HIGH_WATERMARK))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // Encoder: FileTransferPacket → bytes (编码器)
                        pipeline.addLast("encoder", new FileTransferCodec.Encoder());
                        // Decoder: bytes → FileTransferPacket (解码器)
                        pipeline.addLast("decoder", new FileTransferCodec.Decoder());
                        // 实际业务的处理
                        pipeline.addLast("handler",
                                new ClientChannelHandler(filePath, fileId, rateLimiter));
                    }
                });

        ChannelFuture future = bootstrap.connect(host, port).sync();
        if (future.isSuccess()) {
            this.channel = future.channel();
            log.info("Connected to {}:{}, starting transfer of '{}'",
                    host, port, filePath.getFileName());
        }
        return future;
    }

    /**
     * 等待传输完成（阻塞直到通道关闭）。
     */
    public void waitForCompletion() throws InterruptedException {
        if (channel != null) {
            channel.closeFuture().sync();
        }
    }

    /**
     * Stop the client.
     */
    public void stop() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
        log.info("FileTransferClient stopped.");
    }

    /**
     * 发送一个文件并等待其完成。
     * 适用于简单用例的便捷方法。
     *
     * @param host     server host
     * @param port     server port
     * @param filePath path to the file
     */
    public static void sendFile(String host, int port, Path filePath) throws Exception {
        FileTransferClient client = new FileTransferClient();
        try {
            ChannelFuture connect = client.connect(host, port, filePath);
            client.waitForCompletion();
        } finally {
            client.stop();
        }
    }

    /**
     * 根据文件路径和当前时间生成一个文件ID。
     * 在生产环境中，这可以是UUID或文件内容的哈希值。
     */
    private static long generateFileId(Path filePath) {
        long hash = filePath.toAbsolutePath().toString().hashCode();
        long timestamp = System.currentTimeMillis();
        return hash ^ timestamp;
    }

    //------------------------------------------
    // 测试用例
    //------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: FileTransferClient <host> <port> <file-path>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        Path filePath = Paths.get(args[2]);

        sendFile(host, port, filePath);
    }
}
