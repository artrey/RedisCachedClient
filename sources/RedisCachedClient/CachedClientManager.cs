using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace RedisCachedClient
{
    public class CachedClientManager
    {
        private readonly IConnectionMultiplexer multiplexer;
        private readonly Dictionary<int, CachedClient> clients = new Dictionary<int, CachedClient>();

        public CachedClientManager(IConnectionMultiplexer multiplexer)
        {
            this.multiplexer = multiplexer;
            this.multiplexer.ConnectionFailed += (sender, args) =>
            {
                foreach (var client in clients.Values)
                {
                    client.Disconnect();
                }
            };

            this.multiplexer.ConnectionRestored += (sender, args) =>
            {
                foreach (var client in clients.Values)
                {
                    client.Connect();
                }
            };
        }

        public CachedClient GetClient(int databaseId)
        {
            if (clients.TryGetValue(databaseId, out var client)) return client;

            client = new CachedClient(multiplexer.GetDatabase(databaseId));
            clients.Add(databaseId, client);
            return client;
        }

        public List<int> GetActiveDatabases()
        {
            var client = GetClient(0);
            var result = client.Eval("return redis.call('info', 'keyspace')");
            var infoString = result.IsNull ? "" : result.ToString();
            return new Regex(@"db(\d+)").Matches(infoString).Cast<Match>().Select(m =>
            {
                int.TryParse(m.Groups[1].Value, out int db);
                return db;
            }).ToList();
        }
    }
}
