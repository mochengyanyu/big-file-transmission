package com.nettyfile.transfer.protocol;



/**  * 表示文件传输协议中的一个数据包。
 * *
 * <h3>二进制头部布局（56字节固定+可变）</h3>
 *  <pre> * 偏移量 大小 字段 * ------  ----  -----
 *  0       4     魔术数：0x4E465446（"NFTF"）
 *  4       1     版本（高4位）| 数据包类型（低4位）
 *  5       1     标志（位0：lastChunk，位1：retransmit，位2：isAck）
 *  6       2     文件名长度（uint16，大端）
 *  8       8     文件ID（int64，大端）
 *  16      4     块索引（int32，大端）
 *  20      4     载荷大小（int32，大端）
 *  24      8     偏移量（int64，大端） — 文件中的字节偏移量
 *  32      8     下一个预期偏移量（int64，大端） — 用于握手中的ACK/总文件大小
 *  40      16     MD5校验和（16字节） — 针对载荷，若无载荷则补零
 *  56      N     文件名（UTF-8，N = 文件名长度）
 *  56+N    M     载荷（M = 载荷大小）
 *  </pre>
 * */
public class FileTransferPacket {

    // --- Flag bit masks ---
    /** Bit 0: 是否是文件的最后一部分 */
    public static final byte FLAG_LAST_CHUNK = 0x01;
    /** Bit 1: 这是一个重传的数据块 */
    public static final byte FLAG_RETRANSMIT = 0x02;
    /** Bit 2: 此数据包为确认信息 */
    public static final byte FLAG_ACK = 0x04;

    // --- Protocol constants ---
    public static final int MAGIC_NUMBER = 0x4E465446; // "NFTF"
    public static final byte PROTOCOL_VERSION = 0x01;
    public static final int FIXED_HEADER_SIZE = 56;
    public static final int MD5_LENGTH = 16;

    // --- Fields ---
    private byte version = PROTOCOL_VERSION;
    private PacketType packetType;
    private byte flags;
    private short fileNameLength;
    private long fileId;
    private int chunkIndex;
    private int payloadSize;
    private long offset;
    private long nextExpectedOffset;
    private byte[] md5 = new byte[MD5_LENGTH];
    private String fileName;
    private byte[] payload;

    // --- Factory methods ---

    /**
     * 创建一个握手（HANDSHAKE）数据包以启动文件传输。
     */
    public static FileTransferPacket handshake(long fileId, String fileName, long totalFileSize) {
        FileTransferPacket p = new FileTransferPacket();
        p.packetType = PacketType.HANDSHAKE;
        p.fileId = fileId;
        p.fileName = fileName;
        p.nextExpectedOffset = totalFileSize; // carries total file size in handshake
        return p;
    }

    /**
     * 创建一个HANSHAKE_RESPONSE数据包。
     */
    public static FileTransferPacket handshakeResponse(long fileId, String fileName,
                                                        long resumeOffset, long totalFileSize) {
        FileTransferPacket p = new FileTransferPacket();
        p.packetType = PacketType.HANDSHAKE_RESPONSE;
        p.fileId = fileId;
        p.fileName = fileName;
        p.offset = resumeOffset;
        p.nextExpectedOffset = totalFileSize;
        return p;
    }

    /**
     * 创建一个携带文件分块的DATA数据包。
     */
    public static FileTransferPacket data(long fileId, String fileName, int chunkIndex,
                                          long offset, byte[] chunkData, boolean isLastChunk) {
        FileTransferPacket p = new FileTransferPacket();
        p.packetType = PacketType.DATA;
        p.fileId = fileId;
        p.fileName = fileName;
        p.chunkIndex = chunkIndex;
        p.offset = offset;
        p.payload = chunkData;
        p.payloadSize = chunkData != null ? chunkData.length : 0;
        p.md5 = chunkData != null ?
                com.nettyfile.transfer.util.MD5Util.compute(chunkData) : new byte[MD5_LENGTH];
        if (isLastChunk) {
            p.flags |= FLAG_LAST_CHUNK;
        }
        return p;
    }

    /**
     * 创建一个重新传输的数据包（之前数据块的副本）。
     */
    public static FileTransferPacket retransmit(long fileId, String fileName, int chunkIndex,
                                                 long offset, byte[] chunkData, boolean isLastChunk) {
        FileTransferPacket p = data(fileId, fileName, chunkIndex, offset, chunkData, isLastChunk);
        p.flags |= FLAG_RETRANSMIT;
        return p;
    }

    /**
     * 创建一个确认（ACK）数据包。
     * @param receivedOffset 成功接收并写入磁盘的总字节数
     * @param nextExpectedOffset 接收器接下来期望的字节偏移量
     */
    public static FileTransferPacket ack(long fileId, long receivedOffset, long nextExpectedOffset) {
        FileTransferPacket p = new FileTransferPacket();
        p.packetType = PacketType.ACK;
        p.flags |= FLAG_ACK;
        p.fileId = fileId;
        p.offset = receivedOffset;
        p.nextExpectedOffset = nextExpectedOffset;
        return p;
    }

    /**
     * 创建一个NACK数据包，请求重新传输特定数据块。
     */
    public static FileTransferPacket nack(long fileId, int missingChunkIndex, long missingOffset) {
        FileTransferPacket p = new FileTransferPacket();
        p.packetType = PacketType.NACK;
        p.fileId = fileId;
        p.chunkIndex = missingChunkIndex;
        p.offset = missingOffset;
        return p;
    }

    /**
     * 创建一个完整的通知包。
     */
    public static FileTransferPacket complete(long fileId, long totalBytes) {
        FileTransferPacket p = new FileTransferPacket();
        p.packetType = PacketType.COMPLETE;
        p.fileId = fileId;
        p.offset = totalBytes;
        return p;
    }

    // --- Flag helper methods ---
    public boolean isLastChunk() {
        return (flags & FLAG_LAST_CHUNK) != 0;
    }

    public boolean isRetransmit() {
        return (flags & FLAG_RETRANSMIT) != 0;
    }

    public boolean isAck() {
        return (flags & FLAG_ACK) != 0;
    }

    // --- Getters and Setters ---

    public byte getVersion() { return version; }
    public void setVersion(byte version) { this.version = version; }

    public PacketType getPacketType() { return packetType; }
    public void setPacketType(PacketType packetType) { this.packetType = packetType; }

    public byte getFlags() { return flags; }
    public void setFlags(byte flags) { this.flags = flags; }

    public short getFileNameLength() { return fileNameLength; }
    public void setFileNameLength(short fileNameLength) { this.fileNameLength = fileNameLength; }

    public long getFileId() { return fileId; }
    public void setFileId(long fileId) { this.fileId = fileId; }

    public int getChunkIndex() { return chunkIndex; }
    public void setChunkIndex(int chunkIndex) { this.chunkIndex = chunkIndex; }

    public int getPayloadSize() { return payloadSize; }
    public void setPayloadSize(int payloadSize) { this.payloadSize = payloadSize; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public long getNextExpectedOffset() { return nextExpectedOffset; }
    public void setNextExpectedOffset(long nextExpectedOffset) { this.nextExpectedOffset = nextExpectedOffset; }

    public byte[] getMd5() { return md5; }
    public void setMd5(byte[] md5) { this.md5 = md5; }

    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }

    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }

    @Override
    public String toString() {
        return "FileTransferPacket{" +
                "type=" + packetType +
                ", fileId=" + fileId +
                ", chunkIndex=" + chunkIndex +
                ", offset=" + offset +
                ", payloadSize=" + payloadSize +
                ", flags=" + String.format("0x%02X", flags) +
                (isLastChunk() ? " [LAST]" : "") +
                (isRetransmit() ? " [RETX]" : "") +
                ", fileName='" + fileName + '\'' +
                '}';
    }
}
