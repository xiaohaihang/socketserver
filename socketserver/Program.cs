using System.Net;
using System.Net.Sockets;
using socketserver.Service;

namespace socketserver
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            using CancellationTokenSource cts = new();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            using Socket listener = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(new IPEndPoint(IPAddress.Any, 6899));
            listener.Listen(10);
            Console.WriteLine("服务端启动，监听 0.0.0.0:6899");
            Console.WriteLine("按 Ctrl+C 停止服务。");

            try
            {
                while (!cts.IsCancellationRequested)
                {
                    Socket clientSocket = await listener.AcceptAsync(cts.Token);
                    clientSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, 262144);
                    Console.WriteLine($"新客户端接入，远程：{clientSocket.RemoteEndPoint}");
                    _ = RunClientAsync(clientSocket, cts.Token);
                }
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                Console.WriteLine("服务端正在停止。");
            }
        }

        private static async Task RunClientAsync(Socket clientSocket, CancellationToken cancellationToken)
        {
            try
            {
                PriorityTcpServer server = new PriorityTcpServer();
                await server.RunClientAsync(clientSocket, cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"客户端处理异常：{ex.Message}");
            }
            finally
            {
                clientSocket.Dispose();
            }
        }
    }
}
