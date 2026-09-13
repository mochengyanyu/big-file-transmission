package com.nettyfile.transfer.protocol;

/**
 * 数据包类型
 */
public enum PacketType {
    /** Client → Server: 发起文件传输会话 */
    HANDSHAKE((byte) 0x01),
    /** Server → Client: 握手响应，包含恢复偏移量 */
    HANDSHAKE_RESPONSE((byte) 0x02),

    /** Bidirectional: 文件数据块 */
    DATA((byte) 0x03),
    /** Bidirectional: 收到数据的确认 */
    ACK((byte) 0x04),
    /** Server → Client: 请求重传缺失的数据块 */
    NACK((byte) 0x05),
    /** Bidirectional: 传输完成通知 */
    COMPLETE((byte) 0x06);
    private final byte value;
    PacketType(byte value) {
        this.value = value;
    }
    public byte getValue() {
        return value;
    }

    public static PacketType fromValue(byte value) {
        for (PacketType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown packet type: " + value);
    }
}
