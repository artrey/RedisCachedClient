using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
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
        protected readonly IDatabase Database;

        protected readonly ConcurrentObservableDictionary<RedisKey, int> CachedKeys =
            new ConcurrentObservableDictionary<RedisKey, int>();

        // workaround for decrease memory allocating
        private bool _cachedKeysResized = false;
        private RedisKey[] _cachedKeysInternal = new RedisKey[0];
        private readonly List<Task> _tasks = new List<Task>();

        protected RedisKey[] CurrentCachedKeys
        {
            get
            {
                if (_cachedKeysResized)
                {
                    _cachedKeysInternal = CachedKeys.Keys.ToArray();
                    _cachedKeysResized = false;
                }

                return _cachedKeysInternal;
            }
        }

        protected readonly ConcurrentObservableDictionary<string, RedisValue> Cache =
            new ConcurrentObservableDictionary<string, RedisValue>();

        protected readonly ConcurrentDictionary<string, HashSet<Action<RedisChannel, RedisValue>>> ChannelSubs =
            new ConcurrentDictionary<string, HashSet<Action<RedisChannel, RedisValue>>>();

        protected CancellationTokenSource CancelToken;

        protected readonly ISubscriber Subscriber;

        public event EventHandler<RedisChangedEventArgs> DataChanged;

        public int RequestDelay { get; set; }
        public int DatabaseId => Database.Database;
        public bool IsConnected { get; private set; }

        public CachedClient(IDatabase database)
        {
            Database = database;
            Subscriber = database.Multiplexer.GetSubscriber();
            Cache.CollectionChanged += (s, e) => { DataChanged?.Invoke(s, e); };
            CachedKeys.CollectionChanged += (s, e) =>
            {
                if (e.Action == NotifyCollectionChangedAction.Add ||
                    e.Action == NotifyCollectionChangedAction.Remove ||
                    e.Action == NotifyCollectionChangedAction.Reset)
                {
                    _cachedKeysResized = true;
                }
            };
        }

        public bool Connect()
        {
            if (IsConnected) return IsConnected;

            CancelToken = new CancellationTokenSource();
            IsConnected = true;
            Task.Factory.StartNew(UpdateAllDataForeverAsync, CancelToken.Token,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return IsConnected;
        }

        public bool Disconnect()
        {
            if (!IsConnected) return !IsConnected;

            IsConnected = false;
            CancelToken.Cancel();

            return !IsConnected;
        }

        public void ClearCache() => Cache.Clear();

        protected void Subscribe(string key, int count = 1)
        {
            if (key is null) throw new ArgumentNullException(nameof(key));

            CachedKeys.AddOrUpdate(key, 1, (k, i) => i + count);
        }

        protected void Unsubscribe(string key, int count = 1)
        {
            if (key is null) throw new ArgumentNullException(nameof(key));

            if (CachedKeys.AddOrUpdate(key, 0, (k, i) => i - count) <= 0)
            {
                CachedKeys.TryRemove(key, out _);
            }
        }

        public void Subscribe(params string[] keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys)
            {
                Subscribe(key);
            }
        }


        public void Subscribe(IEnumerable<string> keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys)
            {
                Subscribe(key);
            }
        }

        public void Unsubscribe(params string[] keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys)
            {
                Unsubscribe(key);
            }
        }

        public void Unsubscribe(IEnumerable<string> keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys)
            {
                Unsubscribe(key);
            }
        }

        public void UnsubscribeAll() => CachedKeys.Clear();

        public RedisResult Eval(string script, RedisKey[] keys = null,
            RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
            => Database.ScriptEvaluate(script, keys, values, flags);

        public RedisResult Execute(string command, params object[] args) => Database.Execute(command, args);

        public RedisResult Execute(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
            => Database.Execute(command, args, flags);

        public Task<RedisResult> ExecuteAsync(string command, params object[] args)
            => Database.ExecuteAsync(command, args);

        public Task<RedisResult> ExecuteAsync(string command, ICollection<object> args,
            CommandFlags flags = CommandFlags.None) => Database.ExecuteAsync(command, args, flags);

        public RedisValue Get(string key) => Database.StringGet(key);

        public bool Set(string key, RedisValue value, CommandFlags flags = CommandFlags.None)
            => Database.StringSet(key, value, flags: flags);

        public bool SubscribeChannel(string channel, Action<RedisChannel, RedisValue> handler,
            CommandFlags flags = CommandFlags.None)
        {
            if (!ChannelSubs.TryGetValue(channel, out var handlers))
            {
                handlers = new HashSet<Action<RedisChannel, RedisValue>>();
                if (!ChannelSubs.TryAdd(channel, handlers)) return false;
            }

            if (handlers.Contains(handler)) return true;

            Subscriber.Subscribe(channel, handler, flags);
            return handlers.Add(handler);
        }

        public bool UnsubscribeChannel(string channel, Action<RedisChannel, RedisValue> handler,
            CommandFlags flags = CommandFlags.None)
        {
            if (!ChannelSubs.TryGetValue(channel, out var handlers)) return false;
            if (!handlers.Contains(handler)) return false;
            if (!handlers.Remove(handler)) return false;
            Subscriber.Unsubscribe(channel, handler, flags);
            return true;
        }

        public void UnsubscribeAllChannels(CommandFlags flags = CommandFlags.None)
        {
            foreach (var kv in ChannelSubs)
            {
                foreach (var action in kv.Value)
                {
                    Subscriber.Unsubscribe(kv.Key, action, flags);
                }
            }

            ChannelSubs.Clear();
        }

        public long RightPush(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
            => Database.ListRightPush(key, value, flags: flags);

        public RedisValue RightPop(RedisKey key) => Database.ListRightPop(key);

        public long LeftPush(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
            => Database.ListLeftPush(key, value, flags: flags);

        public RedisValue LeftPop(RedisKey key) => Database.ListLeftPop(key);

        public long Publish(string channel, RedisValue value, CommandFlags flags = CommandFlags.None)
            => Database.Publish(channel, value, flags);

        public IDictionary<string, RedisValue> GetAllCachedData() => Cache;

        public bool TryGetCachedData(string key, out RedisValue value) => Cache.TryGetValue(key, out value);

        public void AddPartialObserver(IRedisClientObserver observer, params string[] keys)
            => Subscribe(Cache.AddPartialObserver(observer, keys).Keys);

        public void AddPartialObserver(Action<RedisChangedEventArgs> action, params string[] keys)
            => Subscribe(Cache.AddPartialObserver(action, keys).Keys);

        public void RemovePartialObserver(IRedisClientObserver observer, params string[] keys)
            => Unsubscribe(Cache.RemovePartialObserver(observer, keys).Keys);

        public void RemovePartialObserver(IRedisClientObserver observer)
            => Unsubscribe(Cache.RemovePartialObserver(observer).Keys);

        public void RemovePartialObserver(params string[] keys)
        {
            foreach (var kv in Cache.RemovePartialObserver(keys))
            {
                Unsubscribe(kv.Key, kv.Value.Count);
            }
        }

        public void RemoveAllObservers()
        {
            foreach (var kv in Cache.RemoveAllObservers())
            {
                Unsubscribe(kv.Key, kv.Value.Count);
            }
        }

        protected virtual async Task UpdateAllData()
        {
            var values = await Database.StringGetAsync(CurrentCachedKeys).ConfigureAwait(false);
            for (var i = 0; i < CurrentCachedKeys.Length; ++i)
            {
                Cache.AddOrUpdate(CurrentCachedKeys[i], values[i]);
            }

            foreach (var cacheKey in Cache.Keys)
            {
                if (CachedKeys.ContainsKey(cacheKey)) continue;
                _tasks.Add(Task.Run(() => { Cache.TryRemove(cacheKey, out _); }));
            }

            if (_tasks.Count == 0) return;

            Task.WaitAll(_tasks.ToArray());
            _tasks.Clear();
        }

        private async Task UpdateAllDataForeverAsync()
        {
            while (IsConnected)
            {
                try
                {
                    await UpdateAllData().ConfigureAwait(false);
                    await Task.Delay(RequestDelay).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
        }
    }
}