using System;
using System.Collections.Concurrent;
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
        private readonly ConcurrentDictionary<string, int> _cachedKeys = new ConcurrentDictionary<string, int>();
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

        public void ClearCache()
        {
            _cache.Clear();
        }

        private void Subscribe(string key, int count = 1)
        {
            if (key is null) throw new ArgumentNullException(nameof(key));

            _cachedKeys.AddOrUpdate(key, 1, (k, i) => i + count);
        }

        private void Unsubscribe(string key, int count = 1)
        {
            if (key is null) throw new ArgumentNullException(nameof(key));

            if (_cachedKeys.AddOrUpdate(key, 0, (k, i) => i - count) <= 0)
            {
                _cachedKeys.TryRemove(key, out _);
            }
        }

        public void Subscribe(params string[] keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys) Subscribe(key);
        }

        public void Unsubscribe(params string[] keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys) Unsubscribe(key);
        }

        public void UnsubscribeAll()
        {
            _cachedKeys.Clear();
        }

        public RedisValue Get(string key)
        {
            return _database.StringGet(key);
        }

        public bool Set(string key, RedisValue value)
        {
            return _database.StringSet(key, value);
        }

        public Dictionary<string, RedisValue> GetAllCachedData()
        {
            return _cache.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        public bool TryGetCachedData(string key, out RedisValue value)
        {
            return _cache.TryGetValue(key, out value);
        }

        public void AddPartialObserver(IRedisClientObserver observer, params string[] keys)
        {
            var added = _cache.AddPartialObserver(observer, keys);
            Subscribe(added.Keys.ToArray());
        }

        public void AddPartialObserver(Action<RedisChangedEventArgs> action, params string[] keys)
        {
            var added = _cache.AddPartialObserver(action, keys);
            Subscribe(added.Keys.ToArray());
        }

        public void RemovePartialObserver(IRedisClientObserver observer, params string[] keys)
        {
            var removed = _cache.RemovePartialObserver(observer, keys);
            Unsubscribe(removed.Keys.ToArray());
        }

        public void RemovePartialObserver(IRedisClientObserver observer)
        {
            var removed = _cache.RemovePartialObserver(observer);
            Unsubscribe(removed.Keys.ToArray());
        }

        public void RemovePartialObserver(params string[] keys)
        {
            var removed = _cache.RemovePartialObserver(keys);
            foreach (var kv in removed) Unsubscribe(kv.Key, kv.Value.Count);
        }

        public void RemoveAllObservers()
        {
            var removed = _cache.RemoveAllObservers();
            foreach (var kv in removed) Unsubscribe(kv.Key, kv.Value.Count);
        }

        protected virtual void UpdateAllData()
        {
            var keys = _cachedKeys.Select(k => (RedisKey)k.Key).ToArray();
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
