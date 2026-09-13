package com.nettyfile.transfer.client;

/**
 * 这里是做流控的客户端的这边的，
 * 但是我们一般是在服务端做流控,
 * 利用netty的流量整形，全局的限制
 */
public class RateLimiter {
    /** Default rate: 10 MB/s (80 Mbps). */
    public static final long DEFAULT_BYTES_PER_SECOND = 10 * 1024 * 1024; // 10 MB/s
    private final long bytesPerSecond;
    private final long maxBurstBytes;
    /** 可用令牌（以字节为单位）. */
    private double tokens;
    /** 上次令牌补给的纳秒级时间戳。*/
    private long lastRefillNanos;


    public RateLimiter(long bytesPerSecond, long maxBurstBytes) {
        this.bytesPerSecond = bytesPerSecond;
        this.maxBurstBytes = maxBurstBytes;
        this.tokens = maxBurstBytes;
        this.lastRefillNanos = System.nanoTime();
    }


    public RateLimiter() {
        this(DEFAULT_BYTES_PER_SECOND, 5 * 1024 * 1024); // 5 MB burst
    }


    public synchronized long acquireWithWaitTime(long bytes) {
        refillTokens();
        if (tokens >= bytes) {
            tokens -= bytes;
            return 0;
        }
        double deficit = bytes - tokens;
        tokens = 0;
        return Math.max(1, (long) ((deficit / bytesPerSecond) * 1000.0));
    }

    /**
     * 尝试在不阻塞的情况下获取令牌。
     *
     * @return true if enough tokens were available
     */
    public synchronized boolean tryAcquire(long bytes) {
        refillTokens();
        if (tokens >= bytes) {
            tokens -= bytes;
            return true;
        }
        return false;
    }

    /**
     * 根据经过的时间重新填充代币。
     */
    private void refillTokens() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillNanos;
        lastRefillNanos = now;

        // Tokens to add = rate × elapsed time
        double newTokens = (bytesPerSecond * elapsedNanos) / 1_000_000_000.0;
        tokens = Math.min(tokens + newTokens, maxBurstBytes);
    }

    /**
     * 获取配置的速率（以字节/秒为单位）。
     */
    public long getBytesPerSecond() {
        return bytesPerSecond;
    }

    /**
     * 获取当前可用tokens的数量。
     */
    public synchronized double getAvailableTokens() {
        refillTokens();
        return tokens;
    }
}
