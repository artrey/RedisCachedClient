using System.Collections.Generic;
using StackExchange.Redis;

namespace RedisClient
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
            if (!_clients.TryGetValue(databaseId, out var client))
            {
                client = new CachedClient(_multiplexer.GetDatabase(databaseId));
                _clients.Add(databaseId, client);
            }
            return client;
        }
    }
}
