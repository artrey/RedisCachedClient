using System;
using StackExchange.Redis;
using RedisCachedClient;

namespace RedisClientDemo
{
    internal static class Program
    {
        private static void Main()
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
            t.Execute("flushdb");
            t.DataChanged += (s, e) => { Console.WriteLine($"[{e.Action}] {e.Key}: {e.OldValue} -> {e.NewValue}"); };
            t.AddPartialObserver(new RedisClientObserver(), "test");
            t.AddPartialObserver(
                e => Console.WriteLine($"Auto observer [{e.Action}] {e.Key}: {e.OldValue} -> {e.NewValue}"), "test");
            t.Subscribe("test3");
            t.RequestDelay = 200;
            t.Connect();

            t.Set("test", 123);
            t.Set("test2", 123);
            System.Threading.Thread.Sleep(t.RequestDelay);

            Console.WriteLine(t.GetAllCachedData().Count);

            t.RemovePartialObserver("test");
            System.Threading.Thread.Sleep(t.RequestDelay);

            Console.WriteLine(t.GetAllCachedData().Count);

            var testPubSub = "channelName";

            //t.Set(testPubSub, "1");
            System.Threading.Thread.Sleep(t.RequestDelay);

            t.RightPush(testPubSub, "10");
            t.RightPush(testPubSub, "9");
            t.RightPush(testPubSub, "8");
            t.RightPush(testPubSub, "7");
            System.Threading.Thread.Sleep(t.RequestDelay);
            t.LeftPush(testPubSub, "15");
            // System.Threading.Thread.Sleep(t.RequestDelay);

            while (!t.SubscribeChannel(testPubSub, Handler))
            {
            }

            t.Publish(testPubSub, "testPublishValue");
            Console.WriteLine(t.RightPop(testPubSub));
            Console.WriteLine(t.LeftPop(testPubSub));
            Console.WriteLine(t.LeftPop(testPubSub));
            Console.WriteLine(t.RightPop(testPubSub));
            Console.WriteLine(t.RightPop(testPubSub));
            System.Threading.Thread.Sleep(2000);

            t.Set("test", 255);
            t.ClearCache();

            System.Threading.Thread.Sleep(10000);

            t.Disconnect();
        }

        private static void Handler(RedisChannel arg1, RedisValue arg2)
        {
            Console.WriteLine($"{arg1} : {arg2}");
        }
    }
}