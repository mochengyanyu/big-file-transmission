package com.nettyfile.transfer.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;


/**
 * 自定义的编解码器
 */
public final class FileTransferCodec {


    private static final Logger log = LoggerFactory.getLogger(FileTransferCodec.class);

    private FileTransferCodec() {
        // container for static inner classes
    }

    /**
     * Encodes a {@link FileTransferPacket} into a {@link ByteBuf}.
     * 编码器
     */
    public static class Encoder extends MessageToByteEncoder<FileTransferPacket> {

        @Override
        protected void encode(ChannelHandlerContext ctx, FileTransferPacket packet, ByteBuf out) {
            // -- Fixed header (56 bytes) --
            // Bytes 0-3: 魔术字段
            out.writeInt(FileTransferPacket.MAGIC_NUMBER);
            // Byte 4: Version (版本high 4 bits) | PacketType (数据类型low 4 bits)
            byte versionType = (byte) ((packet.getVersion() << 4)
                    | (packet.getPacketType().getValue() & 0x0F));
            out.writeByte(versionType);
            // Byte 5(状态，是否为最后一块): Flags
            out.writeByte(packet.getFlags());
            // Bytes 6-7: File name length(数据长度)
            byte[] fileNameBytes = packet.getFileName() != null
                    ? packet.getFileName().getBytes(StandardCharsets.UTF_8)
                    : new byte[0];
            out.writeShort(fileNameBytes.length);
            // Bytes 8-15: (文件id)
            out.writeLong(packet.getFileId());
            // Bytes 16-19: Chunk index(当前文件块的索引)
            out.writeInt(packet.getChunkIndex());
            // Bytes 20-23: Payload size(荷载，数据的大小长度)
            int payloadLen = packet.getPayload() != null ?
                    packet.getPayload().length : 0;
            out.writeInt(payloadLen);
            // Bytes 24-31: Offset(文件的偏移量)
            out.writeLong(packet.getOffset());
            // Bytes 32-39: Next expected offset(期望偏移量, 下一个期望的偏移量)
            out.writeLong(packet.getNextExpectedOffset());
            // Bytes 40-55: MD5 checksum(MD5校验)
            byte[] md5 = packet.getMd5();
            if (md5 != null && md5.length == FileTransferPacket.MD5_LENGTH) {
                out.writeBytes(md5);
            } else {
                out.writeZero(FileTransferPacket.MD5_LENGTH);
            }
            // -- Variable header: file name --(数据名称)
            if (fileNameBytes.length > 0) {
                out.writeBytes(fileNameBytes);
            }
            // -- Payload --(荷载，也就是数据)
            if (packet.getPayload() != null && payloadLen > 0) {
                out.writeBytes(packet.getPayload());
            }
        }
    }

    /**
     * 将一个{@link ByteBuf}解码为一个{@link FileTransferPacket}。
     * 解码器读取56字节的固定报头，提取可变长度的字段大小，
     * 并等待所有字节都可用后再构建数据包。这样可以避免部分读取。
     */
    public static class Decoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            // 等待固定的56字节报头
            if (in.readableBytes() < FileTransferPacket.FIXED_HEADER_SIZE) {
                return;
            }
            // 标记当前位置，以便在完整数据包未到达时进行回放
            in.markReaderIndex();
            // --- 读取固定头  ---
            // Magic number
            int magic = in.readInt();
            if (magic != FileTransferPacket.MAGIC_NUMBER) {
                log.error("Invalid magic number: 0x{}, expected 0x{}. Closing connection.",
                        Integer.toHexString(magic).toUpperCase(),
                        Integer.toHexString(FileTransferPacket.MAGIC_NUMBER).toUpperCase());
                ctx.close();
                return;
            }
            // Version + PacketType
            byte versionType = in.readByte();
            byte version = (byte) ((versionType >> 4) & 0x0F);
            byte typeValue = (byte) (versionType & 0x0F);
            PacketType packetType;
            try {
                packetType = PacketType.fromValue(typeValue);
            } catch (IllegalArgumentException e) {
                log.error("不知道的类型: 0x{}. Closing connection.",
                        Integer.toHexString(typeValue & 0xFF));
                ctx.close();
                return;
            }
            // Flags
            byte flags = in.readByte();
            // File name length
            short fileNameLength = in.readShort();
            if (fileNameLength < 0) {
                log.error("Negative file name length: {}. Closing connection.", fileNameLength);
                ctx.close();
                return;
            }
            // File ID
            long fileId = in.readLong();
            // Chunk index
            int chunkIndex = in.readInt();
            // Payload size
            int payloadSize = in.readInt();
            if (payloadSize < 0) {
                log.error("Negative payload size: {}. Closing connection.", payloadSize);
                ctx.close();
                return;
            }
            // Offset
            long offset = in.readLong();
            // Next expected offset
            long nextExpectedOffset = in.readLong();
            // MD5
            byte[] md5 = new byte[FileTransferPacket.MD5_LENGTH];
            in.readBytes(md5);
            // --- 检查变长数据是否完全可用 ---
            int variableBytesNeeded = fileNameLength + payloadSize;
            if (in.readableBytes() < variableBytesNeeded) {
                // 并非所有数据都已到达——请倒退到标记位
                in.resetReaderIndex();
                return;
            }
            // --- Read file name ---
            String fileName = null;
            if (fileNameLength > 0) {
                byte[] fnBytes = new byte[fileNameLength];
                in.readBytes(fnBytes);
                fileName = new String(fnBytes, StandardCharsets.UTF_8);
            }
            // --- Read payload ---
            byte[] payload = null;
            if (payloadSize > 0) {
                payload = new byte[payloadSize];
                in.readBytes(payload);
            }
            // --- Build packet ---
            FileTransferPacket packet = new FileTransferPacket();
            packet.setVersion(version);
            packet.setPacketType(packetType);
            packet.setFlags(flags);
            packet.setFileNameLength(fileNameLength);
            packet.setFileId(fileId);
            packet.setChunkIndex(chunkIndex);
            packet.setPayloadSize(payloadSize);
            packet.setOffset(offset);
            packet.setNextExpectedOffset(nextExpectedOffset);
            packet.setMd5(md5);
            packet.setFileName(fileName);
            packet.setPayload(payload);
            out.add(packet);
        }
    }
}
