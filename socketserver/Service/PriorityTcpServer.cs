using Microsoft.Extensions.ObjectPool;
using socketserver.common;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

namespace socketserver.Service
{
    public class PriorityTcpServer
    {
        #region 核心组件
        // 优先级队列：双 Channel 实现高低优先级分离
        private readonly Channel<BusinessMessage> _highPriorityChannel;
        private readonly Channel<BusinessMessage> _lowPriorityChannel;

        // 业务对象池：复用消息实体
        private readonly ObjectPool<BusinessMessage> _messagePool;

        // 并发控制：信号量控制最大业务处理并发数
        private readonly SemaphoreSlim _processSemaphore;

        // 帧解析器
        private readonly FrameParser _frameParser = new();
        #endregion

        #region 配置参数
        private readonly int _maxConcurrentProcess; // 最大并发处理数
        private readonly int _channelCapacity;      // 单通道容量
        private readonly int _poolMaxRetained;      // 对象池最大保留数
        #endregion

        public PriorityTcpServer(int maxConcurrentProcess = 8, int channelCapacity = 200, int poolMaxRetained = 300)
        {
            _maxConcurrentProcess = maxConcurrentProcess;
            _channelCapacity = channelCapacity;
            _poolMaxRetained = poolMaxRetained;

            // 1. 初始化优先级通道（有界，自带背压）
            _highPriorityChannel = Channel.CreateBounded<BusinessMessage>(
                new BoundedChannelOptions(channelCapacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleWriter = true, // 单写入线程
                    SingleReader = false // 多读取并发
                });

            _lowPriorityChannel = Channel.CreateBounded<BusinessMessage>(
                new BoundedChannelOptions(channelCapacity * 3) // 低优先级容量更大
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleWriter = true,
                    SingleReader = false
                });

            // 2. 初始化对象池
            _messagePool = new DefaultObjectPool<BusinessMessage>(
                new BusinessMessagePoolPolicy(),
                poolMaxRetained);

            // 3. 初始化信号量：控制最大并发处理数
            _processSemaphore = new SemaphoreSlim(maxConcurrentProcess, maxConcurrentProcess);
        }

        /// <summary>
        /// 启动服务：处理单个客户端连接
        /// </summary>
        public async Task RunClientAsync(Socket clientSocket, CancellationToken cancellationToken)
        {
            // 启动接收 + 消费 两个循环
            var receiveTask = ReceiveLoopAsync(clientSocket, cancellationToken);
            var processTask = ProcessSchedulerAsync(cancellationToken);

            try
            {
                await Task.WhenAll(receiveTask, processTask);
            }
            finally
            {
                clientSocket.Dispose();
            }
        }

        #region 接收层：Socket 零拷贝接收 + 解析 + 入队
        private async Task ReceiveLoopAsync(Socket socket, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    // 1. 从数组池租用接收缓冲区（零分配，复用内存）
                    byte[] receiveBuffer = ArrayPool<byte>.Shared.Rent(4096);
                    Memory<byte> bufferMem = receiveBuffer.AsMemory();

                    try
                    {
                        // 2. 零拷贝异步接收
                        int bytesReceived = await socket.ReceiveAsync(bufferMem, cancellationToken);
                        if (bytesReceived == 0)
                        {
                            Log("客户端已断开连接");
                            break; // 对端断开连接
                        }

                        Log($"收到网络数据：{bytesReceived} 字节");

                        // 3. 解析出所有完整帧
                        foreach (ParsedFrame frame in
                                 _frameParser.Feed(receiveBuffer.AsMemory(0, bytesReceived)))
                        {
                            Log($"解析完整帧：MessageId={frame.MessageId}，MessageType={frame.MessageType}，" +
                                $"Priority={frame.Priority}，PayloadLength={frame.Payload.Length}");

                            // 4. 从对象池获取消息实体（复用，无 new）
                            BusinessMessage msg = _messagePool.Get();

                            try
                            {
                                // 5. 填充消息实体
                                msg.MessageId = frame.MessageId;
                                msg.MessageType = frame.MessageType;
                                msg.Priority = frame.Priority;
                                msg.PayloadLength = frame.Payload.Length;

                                // 载荷从数组池租用，拷贝数据（帧解析必须独立存储，避免缓冲区复用覆盖）
                                msg.Payload = ArrayPool<byte>.Shared.Rent(frame.Payload.Length);
                                frame.Payload.CopyTo(msg.Payload, 0);

                                // 6. 根据优先级写入对应通道
                                if (frame.Priority == MessagePriority.High)
                                {
                                    await _highPriorityChannel.Writer.WriteAsync(msg, cancellationToken);
                                }
                                else
                                {
                                    await _lowPriorityChannel.Writer.WriteAsync(msg, cancellationToken);
                                }

                                Log($"消息已进入{(frame.Priority == MessagePriority.High ? "高" : "低")}优先级队列：" +
                                    $"MessageId={msg.MessageId}");
                            }
                            catch
                            {
                                // 解析失败，归还对象避免泄漏
                                _messagePool.Return(msg);
                                throw;
                            }
                        }
                    }
                    finally
                    {
                        // 接收缓冲区用完立即归还池
                        ArrayPool<byte>.Shared.Return(receiveBuffer, clearArray: false);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // 正常取消
            }
            catch (Exception ex)
            {
                Log($"接收处理异常：{ex.Message}");
            }
            finally
            {
                // 标记两个通道写入完成
                _highPriorityChannel.Writer.Complete();
                _lowPriorityChannel.Writer.Complete();
                Log("接收循环已结束，优先级队列已完成写入");
            }
        }
        #endregion

        #region 调度层：优先级调度 + 信号量并发控制
        private async Task ProcessSchedulerAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                BusinessMessage? message = null;
                MessagePriority currentPriority = MessagePriority.Low;

                try
                {
                    // ========== 严格优先级调度：优先读高优先级通道 ==========
                    // 1. 先检查高优先级通道是否有数据
                    if (await _highPriorityChannel.Reader.WaitToReadAsync(cancellationToken))
                    {
                        if (_highPriorityChannel.Reader.TryRead(out message))
                        {
                            currentPriority = MessagePriority.High;
                        }
                    }
                    // 2. 高优先级为空，再读低优先级通道
                    else if (await _lowPriorityChannel.Reader.WaitToReadAsync(cancellationToken))
                    {
                        if (_lowPriorityChannel.Reader.TryRead(out message))
                        {
                            currentPriority = MessagePriority.Low;
                        }
                    }
                    // 3. 两个通道都已完成且无数据，退出调度
                    else
                    {
                        break;
                    }

                    if (message == null)
                        continue;

                    // ========== 信号量控制并发 ==========
                    // 等待可用并发名额，超出则异步等待（不阻塞线程）
                    await _processSemaphore.WaitAsync(cancellationToken);

                    Log($"开始调度消息：MessageId={message.MessageId}，Priority={currentPriority}");

                    // 启动异步处理，不阻塞调度循环
                    _ = ProcessMessageAsync(message, currentPriority, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // 调度异常，记录日志，归还资源
                    if (message != null)
                        _messagePool.Return(message);

                    Console.WriteLine($"调度异常: {ex.Message}");
                }
            }

            // 等待所有在途处理任务完成（简化实现，生产环境建议用计数等待）
            for (int i = 0; i < _maxConcurrentProcess; i++)
            {
                await _processSemaphore.WaitAsync(cancellationToken);
            }
        }
        #endregion

        #region 业务处理层
        private async Task ProcessMessageAsync(BusinessMessage message, MessagePriority priority, CancellationToken cancellationToken)
        {
            try
            {
                Log($"开始处理消息：MessageId={message.MessageId}，MessageType={message.MessageType}，" +
                    $"Priority={priority}，PayloadLength={message.PayloadLength}");

                // ==================== 你的业务逻辑 ====================
                // 直接使用 message.Payload.AsSpan(0, message.PayloadLength) 零拷贝处理
                await HandleBusinessLogicAsync(message, cancellationToken);
                // ====================================================

                Log($"消息处理完成：MessageId={message.MessageId}");
            }
            catch (Exception ex)
            {
                // 单条消息处理失败，异常隔离，不影响整体服务
                Console.WriteLine($"消息[{message.MessageId}]处理失败: {ex.Message}");
            }
            finally
            {
                // 1. 释放信号量名额
                _processSemaphore.Release();

                // 2. 归还消息实体到对象池（自动归还内部字节数组+重置状态）
                _messagePool.Return(message);
            }
        }

        /// <summary>
        /// 业务逻辑实现
        /// </summary>
        private Task HandleBusinessLogicAsync(BusinessMessage message, CancellationToken cancellationToken)
        {
            // 示例：根据消息类型分发处理
            // 使用 Span 零拷贝读取载荷
            ReadOnlySpan<byte> payloadSpan =
                message.Payload is null
                    ? ReadOnlySpan<byte>.Empty
                    : message.Payload.AsSpan(0, message.PayloadLength);

            string payloadText = Encoding.UTF8.GetString(payloadSpan);
            Log($"业务数据：MessageId={message.MessageId}，内容=\"{payloadText}\"");

            return Task.CompletedTask;
        }

        private static void Log(string message)
        {
            Console.WriteLine(
                $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] " +
                $"[线程:{Environment.CurrentManagedThreadId}] {message}");
        }
        #endregion
    }

}
