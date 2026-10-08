using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace socketserver.common
{

    public enum MessagePriority
    {
        High = 0, // 高优先级：控制指令、心跳、关键响应
        Low = 1   // 低优先级：普通业务数据、批量上报
    }
    public class BusinessMessage
    {
        public long MessageId { get; set; }
        public int MessageType { get; set; }
        public MessagePriority Priority { get; set; }
        /// <summary>
        /// 业务载荷，从数组池租用，用完归还
        /// </summary>
        public byte[]? Payload { get; set; }
        /// <summary>
        /// 载荷实际有效长度
        /// </summary>
        public int PayloadLength { get; set; }
    }
    public class BusinessMessagePoolPolicy : IPooledObjectPolicy<BusinessMessage>
    {
        public BusinessMessage Create()
        {
            return new BusinessMessage();
        }

        public bool Return(BusinessMessage obj)
        {
            // 1. 先归还内部的字节数组到数组池
            if (obj.Payload != null)
            {
                ArrayPool<byte>.Shared.Return(obj.Payload, clearArray: false);
                obj.Payload = null;
            }

            // 2. 重置所有字段状态，避免脏数据
            obj.MessageId = 0;
            obj.MessageType = 0;
            obj.Priority = MessagePriority.Low;
            obj.PayloadLength = 0;

            return true;
        }
    }
    /// <summary>
    /// TCP 帧解析器（长度前缀协议）
    /// 协议格式：[4字节体长度(小端) + 1字节优先级 + 8字节ID + 4字节类型 + N字节载荷]
    /// </summary>
    public class FrameParser
    {
        private byte[] _buffer = Array.Empty<byte>();
        private int _writePos;
        private const int HeaderLength = 17; // 4长度 + 1优先级 + 8ID + 4类型 = 17字节头

        /// <summary>
        /// 喂入字节流，返回所有解析完成的完整帧头+载荷视图
        /// </summary>
        public IEnumerable<ParsedFrame> Feed(ReadOnlyMemory<byte> newData)
        {
            EnsureCapacity(newData.Length);
            newData.Span.CopyTo(_buffer.AsSpan(_writePos));
            _writePos += newData.Length;

            while (_writePos >= HeaderLength)
            {
                int bodyLength = BinaryPrimitives.ReadInt32LittleEndian(
                    _buffer.AsSpan(0, 4));

                if (bodyLength < HeaderLength - 4)
                {
                    throw new InvalidDataException("帧长度无效。");
                }

                int totalFrameLength = 4 + bodyLength;

                if (_writePos < totalFrameLength)
                    break;

                var frameSpan = _buffer.AsSpan(4, bodyLength);
                var priority = (MessagePriority)frameSpan[0];
                long msgId = BinaryPrimitives.ReadInt64LittleEndian(frameSpan.Slice(1, 8));
                int msgType = BinaryPrimitives.ReadInt32LittleEndian(frameSpan.Slice(9, 4));
                byte[] payload = frameSpan.Slice(13).ToArray();

                yield return new ParsedFrame
                {
                    Priority = priority,
                    MessageId = msgId,
                    MessageType = msgType,
                    Payload = payload
                };

                int remain = _writePos - totalFrameLength;
                if (remain > 0)
                {
                    _buffer.AsSpan(totalFrameLength, remain).CopyTo(_buffer);
                }
                _writePos = remain;
            }
        }

        private void EnsureCapacity(int incomingLength)
        {
            int requiredLength = _writePos + incomingLength;
            if (requiredLength <= _buffer.Length)
                return;

            int newSize = Math.Max(_buffer.Length * 2, requiredLength);
            Array.Resize(ref _buffer, newSize);
        }
    }

    public sealed class ParsedFrame
    {
        public MessagePriority Priority { get; init; }
        public long MessageId { get; init; }
        public int MessageType { get; init; }
        public byte[] Payload { get; init; } = Array.Empty<byte>();

        public string GetPayloadString()
        {
            return Encoding.UTF8.GetString(Payload);
        }
    }
}
