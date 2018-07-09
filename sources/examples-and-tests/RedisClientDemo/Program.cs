using System;
using StackExchange.Redis;
using RedisCachedClient;

namespace RedisClientDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var configuration = new ConfigurationOptions
            {
                EndPoints =
                {
                    {"127.0.0.1", 6379}
                },
                KeepAlive = 60,
                AbortOnConnectFail = true,
                ConnectTimeout = 5000,
                ConnectRetry = 5,
                ReconnectRetryPolicy = new LinearRetry(500),
                DefaultDatabase = 0,
                AllowAdmin = true
            };

            var conn = ConnectionMultiplexer.Connect(configuration);
            var man = new CachedClientManager(conn);

            var t = man.GetClient(0);
            t.DataChanged += (s, e) => { Console.WriteLine($"[{e.Action}] {e.Key}: {e.OldValue} -> {e.NewValue}"); };
            t.RequestDelay = 200;
            t.Connect();

            t.Set("test", 123);

            Console.WriteLine(t.GetAllCachedData().Count);

            System.Threading.Thread.Sleep(15000);
            t.Disconnect();
        }
    }
}
