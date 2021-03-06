using System;
using System.Threading;
using StackExchange.Redis;
using Xunit;

namespace RedisCachedClient.Tests
{
    public class ConnectionMultiplexerFixture : IDisposable
    {
        public readonly ConnectionMultiplexer Multiplexer;
        
        public ConnectionMultiplexerFixture()
        {
            var configuration = new ConfigurationOptions
            {
                EndPoints =
                {
                    {"127.0.0.1", 6379}
                },
                KeepAlive = 60,
                AbortOnConnectFail = true,
                ConnectTimeout = 1000,
                ConnectRetry = 1,
                ReconnectRetryPolicy = new LinearRetry(500),
                DefaultDatabase = 0,
                AllowAdmin = true
            };

            Multiplexer = ConnectionMultiplexer.Connect(configuration);
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Multiplexer.Close();
            Multiplexer.Dispose();
        }
    }
    
    public class CachedClientTest : IClassFixture<ConnectionMultiplexerFixture>, IDisposable
    {
        private readonly CachedClient _client;
        
        public CachedClientTest(ConnectionMultiplexerFixture multiplexerFixture)
        {
            var manager = new CachedClientManager(multiplexerFixture.Multiplexer);
            _client = manager.GetClient(0);
            _client.Execute("flushdb");
            _client.RequestDelay = 10;
            _client.Connect();
        }
        
        [Fact]
        public void DatabaseId_Get_0Returned()
        {
            Assert.Equal(0, _client.DatabaseId);
        }
        
        [Fact]
        public void IsConnected_Get_TrueReturned()
        {
            Assert.True(_client.IsConnected);
        }
        
        [Fact]
        public void DataChanged_Occur_TrueReturned()
        {
            var key = "testKey";
            var value = "testValue";
            var occured = false;
            _client.Subscribe(key);
            _client.DataChanged += (_, e) =>
            {
                occured = e.Key == key && e.NewValue == value;
            };
            _client.Set(key, value);
            Thread.Sleep(2 * _client.RequestDelay);
            Assert.True(occured);
        }
        
        [Fact]
        public void ClientObserver_Occur_TrueReturned()
        {
            var observer = new TestRedisClientObserver("testKey", "testValue");
            _client.AddPartialObserver(observer, observer.Key);
            _client.Set(observer.Key, observer.Value);
            Thread.Sleep(2 * _client.RequestDelay);
            Assert.True(observer.Occured);
        }
        
        [Fact]
        public void ActionObserver_Occur_TrueReturned()
        {
            var key = "testKey";
            var value = "testValue";
            var occured = false;
            _client.AddPartialObserver(e =>
            {
                occured = e.Key == key && e.NewValue == value;
            }, key);
            _client.Set(key, value);
            Thread.Sleep(2 * _client.RequestDelay);
            Assert.True(occured);
        }
        
        [Fact]
        public void RemovePartialObserver_RemoveByObserverAndKey_FalseReturned()
        {
            var observer = new TestRedisClientObserver("test_key", "test_value");
            _client.AddPartialObserver(observer, observer.Key);
            _client.RemovePartialObserver(observer, observer.Key);
            _client.Set(observer.Key, observer.Value);
            Thread.Sleep(2 * _client.RequestDelay);
            Assert.False(observer.Occured);
        }
        
        [Fact]
        public void RemovePartialObserver_RemoveByKey_FalseReturned()
        {
            var observer = new TestRedisClientObserver("test_key", "test_value");
            _client.AddPartialObserver(observer, observer.Key);
            _client.RemovePartialObserver(observer.Key);
            _client.Set(observer.Key, observer.Value);
            Thread.Sleep(2 * _client.RequestDelay);
            Assert.False(observer.Occured);
        }
        
        [Fact]
        public void Channel_PubSub_TrueReturned()
        {
            var channelName = "testChannel";
            var value = "testValue";
            var occured = false;
            _client.SubscribeChannel(channelName, (c, v) =>
            {
                occured = c == channelName && v == value;
            });
            _client.Publish(channelName, value);
            Thread.Sleep(2 * _client.RequestDelay);
            Assert.True(occured);
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            _client.Disconnect();
            _client.ClearCache();
            _client.UnsubscribeAll();
            _client.UnsubscribeAllChannels();
        }
    }
}
