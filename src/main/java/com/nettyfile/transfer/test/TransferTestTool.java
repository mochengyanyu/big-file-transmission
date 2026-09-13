package com.nettyfile.transfer.test;

import com.nettyfile.transfer.client.FileSender;
import com.nettyfile.transfer.client.FileTransferClient;
import com.nettyfile.transfer.client.RateLimiter;
import com.nettyfile.transfer.util.MD5Util;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Random;

/**
 * 端到端传输测试工具（NACK 已验证通过后，用这个验证「正常链路」）。
 *
 * <p>它走的是<b>生产链路</b>：{@link FileTransferClient} → ClientChannelHandler →
 * {@link FileSender}，不是另写一套发送逻辑，所以测出来的结果就是真实行为。</p>
 *
 * <h3>用法</h3>
 * <pre>
 * # ① 只生成测试文件（默认 33.5 MB，故意不是 1MB 整数倍，用来覆盖最后一个"半块"）
 * java com.nettyfile.transfer.test.TransferTestTool gen ./test-data/big.bin
 * java com.nettyfile.transfer.test.TransferTestTool gen ./test-data/big.bin 200
 *
 * # ② 只发送（注意：要先启动 FileTransferServer）
 * java com.nettyfile.transfer.test.TransferTestTool send localhost 8787 ./test-data/big.bin
 *
 * # ③ 一条龙：生成 + 发送 + 打印校验信息
 * java com.nettyfile.transfer.test.TransferTestTool all localhost 8787 ./test-data/big.bin 50
 *
 * # ④ 校验：源文件 vs 服务端落盘文件（比对长度 + MD5）
 * java com.nettyfile.transfer.test.TransferTestTool verify ./test-data/big.bin "C:/Users/14827/Desktop/big.bin"
 * </pre>
 *
 * <p>send / all 的最后一个可选参数是限速（MB/s），默认 10（与服务端 80Mbps 预留一致）；
 * 传 0 表示关闭客户端令牌桶，用本地环回压测一下真实吞吐。</p>
 */
public final class TransferTestTool {

    /** 与 FileSender 完全一致的块大小：1MB，保证测试覆盖真实的块边界 */
    private static final int CHUNK_SIZE = FileSender.CHUNK_SIZE;

    /** 每个块头部写入 8 字节"块序号戳"，便于在落盘文件里定位错位/丢块 */
    private static final int STAMP_SIZE = 8;

    /** 生成文件的默认大小（MB）：故意取非整数倍，覆盖最后一个不足 1MB 的块 */
    private static final double DEFAULT_SIZE_MB = 33.5;

    /** 默认服务端端口，与 FileTransferServer.main 保持一致 */
    private static final int DEFAULT_PORT = 8787;

    /** 默认客户端限速（MB/s），与服务端 10MB/s 入站限速对齐 */
    private static final double DEFAULT_RATE_MBPS = 10.0;

    private TransferTestTool() {

    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }

        String cmd = args[0].toLowerCase(Locale.ROOT);
        switch (cmd) {
            case "gen": {
                Path file = Paths.get(require(args, 1, "file"));
                double sizeMb = args.length >= 3 ? Double.parseDouble(args[2]) : DEFAULT_SIZE_MB;
                generate(file, toBytes(sizeMb));
                break;
            }
            case "send": {
                String host = require(args, 1, "host");
                int port = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_PORT;
                Path file = Paths.get(require(args, 3, "file"));
                double rateMb = args.length >= 5 ? Double.parseDouble(args[4]) : DEFAULT_RATE_MBPS;
                send(host, port, file, rateMb);
                break;
            }
            case "all": {
                String host = require(args, 1, "host");
                int port = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_PORT;
                Path file = Paths.get(require(args, 3, "file"));
                double sizeMb = args.length >= 5 ? Double.parseDouble(args[4]) : DEFAULT_SIZE_MB;
                double rateMb = args.length >= 6 ? Double.parseDouble(args[5]) : DEFAULT_RATE_MBPS;
                generate(file, toBytes(sizeMb));
                System.out.println();
                send(host, port, file, rateMb);
                break;
            }
            case "verify": {
                Path src = Paths.get(require(args, 1, "srcFile"));
                Path dst = Paths.get(require(args, 2, "receivedFile"));
                verify(src, dst);
                break;
            }
            default:
                usage();
        }
    }

    // ========================================================================
    // ① 生成测试文件
    // ========================================================================

    /**
     * 生成一个内容可复现的测试文件。
     *
     * <p>数据按 1MB 分块填充，每块以「块序号」为随机种子，并在块头写入 8 字节块序号戳。
     * 这样一旦服务端落盘的字节错位（比如 offset 算错、丢块），直接在二进制里就能看出来，
     * 而不用只看 MD5 不等。</p>
     *
     * @param target     目标文件
     * @param totalBytes 文件总字节数
     * @return 源文件的 MD5 十六进制串
     */
    private static String generate(Path target, long totalBytes) throws IOException {
        if (totalBytes <= 0) {
            throw new IllegalArgumentException("size must be > 0, got " + totalBytes + " bytes");
        }

        Path abs = target.toAbsolutePath();
        if (abs.getParent() != null) {
            Files.createDirectories(abs.getParent());
        }

        long totalChunks = (totalBytes + CHUNK_SIZE - 1) / CHUNK_SIZE;
        System.out.println("══════════════════════════════════════════════════");
        System.out.println("  生成测试文件");
        System.out.println("  路径   : " + abs);
        System.out.println("  大小   : " + formatSize(totalBytes) + " (" + totalBytes + " bytes)");
        System.out.println("  块数   : " + totalChunks + " × 1MB"
                + (totalBytes % CHUNK_SIZE != 0
                        ? "  [末块 " + (totalBytes % CHUNK_SIZE) + " bytes]" : ""));
        System.out.println("══════════════════════════════════════════════════");

        MessageDigest md5 = newMd5();
        long startNanos = System.nanoTime();
        long offset = 0;

        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(abs), CHUNK_SIZE)) {
            while (offset < totalBytes) {
                int len = (int) Math.min(CHUNK_SIZE, totalBytes - offset);
                int chunkIndex = (int) (offset / CHUNK_SIZE);

                byte[] buf = new byte[len];
                new Random(chunkIndex * 31L + 1).nextBytes(buf);
                stampChunkIndex(buf, chunkIndex);

                out.write(buf);
                md5.update(buf);

                offset += len;

                boolean isLast = offset >= totalBytes;
                if (isLast || chunkIndex % 16 == 0) {
                    System.out.printf("   ... %,d / %,d bytes (%d%%)%n",
                            offset, totalBytes, offset * 100 / totalBytes);
                }
            }
        }

        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        String md5Hex = MD5Util.bytesToHex(md5.digest());

        System.out.println("  ✅ 生成完成，耗时 " + elapsedMs + " ms");
        System.out.println("  MD5    : " + md5Hex);
        System.out.println("  (服务端收到的文件 MD5 必须与这个值完全一致)");
        System.out.println("══════════════════════════════════════════════════");
        return md5Hex;
    }

    // ========================================================================
    // ② 发送（走生产链路）
    // ========================================================================

    /**
     * 用生产链路发送文件：FileTransferClient.connect → 握手 → FileSender 分块发送 → 等待 ACK 收尾。
     *
     * @param rateMb 客户端令牌桶限速（MB/s）；<= 0 表示关闭限速
     */
    private static void send(String host, int port, Path file, double rateMb) throws Exception {
        if (!Files.isRegularFile(file)) {
            throw new IllegalArgumentException("Not a regular file: " + file.toAbsolutePath());
        }

        long fileSize = Files.size(file);
        long totalChunks = (fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE;

        System.out.println("══════════════════════════════════════════════════");
        System.out.println("  发送测试文件（生产链路：FileTransferClient + FileSender）");
        System.out.println("  目标   : " + host + ":" + port);
        System.out.println("  文件   : " + file.toAbsolutePath().getFileName());
        System.out.println("  大小   : " + formatSize(fileSize) + " (" + fileSize + " bytes, " + totalChunks + " chunks)");
        System.out.println("  限速   : " + (rateMb <= 0
                ? "关闭客户端令牌桶（服务端仍有 10MB/s 入站整形）"
                : rateMb + " MB/s 令牌桶"));
        System.out.println("  源 MD5 : " + md5Of(file));
        System.out.println("══════════════════════════════════════════════════");

        FileTransferClient client = rateMb <= 0
                ? new FileTransferClient(null)
                : new FileTransferClient(
                        new RateLimiter((long) (rateMb * 1024 * 1024), 5L * 1024 * 1024));

        long startNanos = System.nanoTime();
        try {
            client.connect(host, port, file);
            client.waitForCompletion();
        } finally {
            client.stop();
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        System.out.println("══════════════════════════════════════════════════");
        System.out.println("  ✅ 发送结束（连接已关闭）");
        System.out.println("  耗时   : " + elapsedMs + " ms"
                + (elapsedMs > 0 ? "  (≈ " + String.format("%.2f", fileSize / 1024.0 / 1024.0 / (elapsedMs / 1000.0)) + " MB/s)" : ""));
        System.out.println("  下一步 : 到服务端输出目录确认文件大小与 MD5，或直接执行 verify 子命令比对");
        System.out.println("══════════════════════════════════════════════════");
    }

    // ========================================================================
    // ③ 校验：源文件 vs 服务端落盘文件
    // ========================================================================

    private static void verify(Path src, Path dst) throws IOException {
        if (!Files.isRegularFile(src)) {
            throw new IllegalArgumentException("Source file not found: " + src.toAbsolutePath());
        }
        if (!Files.isRegularFile(dst)) {
            throw new IllegalArgumentException("Received file not found: " + dst.toAbsolutePath());
        }

        long srcSize = Files.size(src);
        long dstSize = Files.size(dst);
        String srcMd5 = md5Of(src);
        String dstMd5 = md5Of(dst);

        System.out.println("══════════════════════════════════════════════════");
        System.out.println("  完整性校验");
        System.out.printf("  源文件  : %-40s%n", src.toAbsolutePath());
        System.out.printf("             size=%d, md5=%s%n", srcSize, srcMd5);
        System.out.printf("  收到文件: %-40s%n", dst.toAbsolutePath());
        System.out.printf("             size=%d, md5=%s%n", dstSize, dstMd5);
        System.out.println("──────────────────────────────────────────────────");

        boolean ok = (srcSize == dstSize) && srcMd5.equals(dstMd5);
        if (ok) {
            System.out.println("  ✅ 校验通过：长度与 MD5 完全一致");
        } else {
            System.out.println("  ❌ 校验失败");
            if (srcSize != dstSize) {
                System.out.println("     长度不一致: 期望 " + srcSize + ", 实际 " + dstSize);
            }
            if (!srcMd5.equals(dstMd5)) {
                System.out.println("     MD5 不一致:  期望 " + srcMd5 + ", 实际 " + dstMd5);
            }
        }
        System.out.println("══════════════════════════════════════════════════");
    }

    // ========================================================================
    // 辅助
    // ========================================================================

    /** 在块首写入 8 字节块序号（大端），落盘文件里可直接定位错位 */
    private static void stampChunkIndex(byte[] buf, int chunkIndex) {
        if (buf.length < STAMP_SIZE) {
            return;
        }
        buf[0] = (byte) (chunkIndex >>> 24);
        buf[1] = (byte) (chunkIndex >>> 16);
        buf[2] = (byte) (chunkIndex >>> 8);
        buf[3] = (byte) chunkIndex;
        buf[4] = 'C';
        buf[5] = 'H';
        buf[6] = 'K';
        buf[7] = 0x00;
    }

    /** 流式计算文件 MD5（不整文件载入内存） */
    private static String md5Of(Path file) throws IOException {
        MessageDigest md5 = newMd5();
        byte[] buf = new byte[CHUNK_SIZE];
        try (InputStream in = Files.newInputStream(file)) {
            int n;
            while ((n = in.read(buf)) > 0) {
                md5.update(buf, 0, n);
            }
        }
        return MD5Util.bytesToHex(md5.digest());
    }

    private static MessageDigest newMd5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    private static long toBytes(double sizeMb) {
        return (long) (sizeMb * 1024 * 1024);
    }

    private static String formatSize(long bytes) {
        if (bytes >= 1024L * 1024 * 1024) {
            return String.format("%.2f GB", bytes / 1024.0 / 1024.0 / 1024.0);
        }
        return String.format("%.2f MB", bytes / 1024.0 / 1024.0);
    }

    private static String require(String[] args, int index, String name) {
        if (args.length <= index) {
            System.err.println("缺少参数: <" + name + ">");
            usage();
            throw new IllegalArgumentException("Missing argument: " + name);
        }
        return args[index];
    }

    private static void usage() {
        System.out.println("端到端传输测试工具");
        System.out.println();
        System.out.println("  gen    <file> [sizeMb]                      生成测试文件（默认 "
                + DEFAULT_SIZE_MB + " MB）");
        System.out.println("  send   <host> <port> <file> [rateMBps]      发送文件（rateMBps=0 关闭客户端限速）");
        System.out.println("  all    <host> <port> <file> [sizeMb] [rateMBps]  生成 + 发送");
        System.out.println("  verify <srcFile> <receivedFile>             比对长度与 MD5");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  java -cp target/classes com.nettyfile.transfer.test.TransferTestTool \\");
        System.out.println("       all localhost 8787 ./test-data/big.bin 50 0");
    }
}
