using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;

namespace socketclient
{
    internal class Program
    {
        private const int Port = 6899;

        private enum TestPriority : byte
        {
            High = 0,
            Low = 1
        }

        static async Task Main(string[] args)
        {
            string host = args.Length > 0 ? args[0] : "127.0.0.1";
            using Socket socketClient = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            try
            {
                await socketClient.ConnectAsync(host, Port);
                Console.WriteLine($"已连接服务端 {host}:{Port}");

                for (int i = 1; i <= 5; i++)
                {
                    TestPriority priority = i % 2 == 0 ? TestPriority.High : TestPriority.Low;
                    string text = $"第 {i} 条测试消息，优先级：{priority}";
                    byte[] frame = CreateFrame(priority, i, 1001, Encoding.UTF8.GetBytes(text));

                    await SendAllAsync(socketClient, frame);
                    Console.WriteLine($"已发送完整帧：ID={i}，Priority={priority}，Payload={text}");
                    await Task.Delay(300);
                }

                Console.WriteLine("测试完成，按回车键退出客户端。");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"连接异常：{ex.Message}");
            }
        }

        private static byte[] CreateFrame(TestPriority priority, long messageId, int messageType, byte[] payload)
        {
            const int bodyHeaderLength = 13;
            int bodyLength = bodyHeaderLength + payload.Length;
            byte[] frame = new byte[4 + bodyLength];

            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), bodyLength);
            frame[4] = (byte)priority;
            BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(5, 8), messageId);
            BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(13, 4), messageType);
            payload.CopyTo(frame, 17);
            return frame;
        }

        private static async Task SendAllAsync(Socket socket, byte[] data)
        {
            int sent = 0;
            while (sent < data.Length)
            {
                sent += await socket.SendAsync(data.AsMemory(sent), SocketFlags.None);
            }
        }

    }
}
