using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConcurrentObservableCollections.ConcurrentObservableDictionary;
using StackExchange.Redis;

namespace RedisCachedClient
{
    using RedisChangedEventArgs = DictionaryChangedEventArgs<string, RedisValue>;

    public class CachedClient
    {
        private readonly IDatabase _database;
        private readonly HashSet<string> _cachedKeys = new HashSet<string>(); // ConcurrentHashSet not exists :(
        private readonly ConcurrentObservableDictionary<string, RedisValue> _cache = new ConcurrentObservableDictionary<string, RedisValue>();
        private CancellationTokenSource _cancelToken;

        public event EventHandler<RedisChangedEventArgs> DataChanged;

        public int RequestDelay { get; set; }
        public int DatabaseId => _database.Database;
        public bool IsConnected { get; private set; }

        public CachedClient(IDatabase database)
        {
            _database = database;
            _cache.CollectionChanged += (sender, e) => { DataChanged?.Invoke(sender, e); };
        }

        public bool Connect()
        {
            if (IsConnected) return IsConnected;

            _cancelToken = new CancellationTokenSource();
            IsConnected = true;
            Task.Factory.StartNew(UpdateAllDataForeverAsync, _cancelToken.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return IsConnected;
        }

        public bool Disconnect()
        {
            if (!IsConnected) return !IsConnected;

            IsConnected = false;
            _cancelToken.Cancel();

            return !IsConnected;
        }

        public void Subscribe(params string[] keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys) _cachedKeys.Add(key);
        }

        public void Unsubscribe(params string[] keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys) _cachedKeys.Remove(key);
        }

        public RedisValue Get(string key)
        {
            if (!IsConnected) return RedisValue.Null;

            Subscribe(key);
            var val = _database.StringGet(key);
            return val.HasValue ? _cache.AddOrUpdate(key, val) : val;
        }

        public bool Set(string key, RedisValue value)
        {
            if (!IsConnected) return false;

            var ret = _database.StringSet(key, value);
            if (ret)
            {
                Subscribe(key);
                _cache.AddOrUpdate(key, value);
            }
            return ret;
        }

        public Dictionary<string, RedisValue> GetAllCachedData()
        {
            return _cache.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        public bool TryGetCachedData(string key, out RedisValue value)
        {
            return _cache.TryGetValue(key, out value);
        }

        public bool AddPartialObserver(IRedisClientObserver observer, params string[] keys)
        {
            var ret = !(_cache.AddPartialObserver(observer, keys) is null);
            if (ret)
            {
                Subscribe(keys);
            }
            return ret;
        }

        public bool AddPartialObserver(Action<RedisChangedEventArgs> action, params string[] keys)
        {
            var ret = !(_cache.AddPartialObserver(action, keys) is null);
            if (ret)
            {
                Subscribe(keys);
            }
            return ret;
        }

        public bool RemovePartialObserver(IRedisClientObserver observer, params string[] keys)
        {
            return _cache.RemovePartialObserver(observer, keys).Count > 0;
        }

        public bool RemovePartialObserver(IRedisClientObserver observer)
        {
            return _cache.RemovePartialObserver(observer).Count > 0;
        }

        public bool RemovePartialObserver(params string[] keys)
        {
            Unsubscribe(keys);
            return _cache.RemovePartialObserver(keys).Count > 0;
        }

        protected virtual void UpdateAllData()
        {
            var keys = _cachedKeys.Select(k => (RedisKey)k).ToArray();
            var values = _database.StringGet(keys);

            Task.WaitAll(
                // adding and updating
                keys.Select((t, i) => i).Select(j => Task.Run(() => _cache.AddOrUpdate(keys[j], values[j]))).Cast<Task>()
                // and removing
                .Union(_cache.Keys.Except(keys.Select(k => (string)k)).Select(key => Task.Run(() => _cache.TryRemove(key, out _)))).ToArray()
            );
        }

        private async Task UpdateAllDataForeverAsync()
        {
            while (IsConnected)
            {
                try
                {
                    UpdateAllData();
                    await Task.Delay(RequestDelay);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
        }
    }
}
