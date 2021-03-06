using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace RedisCachedClient
{
    public class CachedClientManager
    {
        private readonly IConnectionMultiplexer _multiplexer;
        private readonly Dictionary<int, CachedClient> _clients = new Dictionary<int, CachedClient>();

        public CachedClientManager(IConnectionMultiplexer multiplexer)
        {
            _multiplexer = multiplexer;
            _multiplexer.ConnectionFailed += (sender, args) =>
            {
                foreach (var client in _clients.Values)
                {
                    client.Disconnect();
                }
            };
            _multiplexer.ConnectionRestored += (sender, args) =>
            {
                foreach (var client in _clients.Values)
                {
                    client.Connect();
                }
            };
        }

        public CachedClient GetClient(int databaseId)
        {
            if (_clients.TryGetValue(databaseId, out var client)) return client;

            client = new CachedClient(_multiplexer.GetDatabase(databaseId));
            _clients.Add(databaseId, client);
            return client;
        }

        public List<int> GetActiveDatabases()
        {
            var client = GetClient(0);
            var result = client.Eval("return redis.call('info', 'keyspace')");
            var infoString = result.IsNull ? "" : result.ToString();
            return new Regex(@"db(\d+)").Matches(infoString).Cast<Match>()
                .Select(m => Convert.ToInt32(m.Groups[1].Value)).ToList();
        }
    }
}