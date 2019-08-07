using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
        protected readonly ConcurrentDictionary<RedisKey, int> CachedKeys = new ConcurrentDictionary<RedisKey, int>();
        protected readonly ConcurrentObservableDictionary<string, RedisValue> Cache = new ConcurrentObservableDictionary<string, RedisValue>();
        protected CancellationTokenSource CancelToken;

        protected ISubscriber redisSub;

        public event EventHandler<RedisChangedEventArgs> DataChanged;

        public int RequestDelay { get; set; }
        public int DatabaseId => Database.Database;
        public bool IsConnected { get; private set; }

        public CachedClient(IDatabase database)
        {
            Database = database;
            redisSub = database.Multiplexer.GetSubscriber();
            Cache.CollectionChanged += (sender, e) => { DataChanged?.Invoke(sender, e); };
        }

        public bool Connect()
        {
            if (IsConnected) return IsConnected;

            CancelToken = new CancellationTokenSource();
            IsConnected = true;
            Task.Factory.StartNew(UpdateAllDataForeverAsync, CancelToken.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return IsConnected;
        }

        public bool Disconnect()
        {
            if (!IsConnected) return !IsConnected;

            IsConnected = false;
            CancelToken.Cancel();

            return !IsConnected;
        }

        public void ClearCache()
        {
            Cache.Clear();
        }

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

            foreach (var key in keys) Subscribe(key);
        }


        public void Subscribe(IEnumerable<string> keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys) Subscribe(key);
        }

        public void Unsubscribe(params string[] keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys) Unsubscribe(key);
        }

        public void Unsubscribe(IEnumerable<string> keys)
        {
            if (keys is null) throw new ArgumentNullException(nameof(keys));

            foreach (var key in keys) Unsubscribe(key);
        }

        public void UnsubscribeAll()
        {
            CachedKeys.Clear();
        }

        public RedisResult Eval(string script, RedisKey[] keys = null, RedisValue[] values = null,
            CommandFlags flags = CommandFlags.None)
        {
            return Database.ScriptEvaluate(script, keys, values, flags);
        }

        public RedisResult Execute(string command, params object[] args)
        {
            return Database.Execute(command, args);
        }

        public RedisResult Execute(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
        {
            return Database.Execute(command, args, flags);
        }

        public Task<RedisResult> ExecuteAsync(string command, params object[] args)
        {
            return Database.ExecuteAsync(command, args);
        }

        public Task<RedisResult> ExecuteAsync(string command, ICollection<object> args,
            CommandFlags flags = CommandFlags.None)
        {
            return Database.ExecuteAsync(command, args, flags);
        }

        public RedisValue Get(string key)
        {
            return Database.StringGet(key);
        }

        public bool Set(string key, RedisValue value)
        {
            return Database.StringSet(key, value);
        }

        protected readonly ConcurrentDictionary<string, HashSet<Action<RedisChannel, RedisValue>>> ChannelSubs = new ConcurrentDictionary<string, HashSet<Action<RedisChannel, RedisValue>>>();

        public bool SubscribeChannel(string channel, Action<RedisChannel, RedisValue> handler)
        {

            if (ChannelSubs.ContainsKey(channel))
            {
                if (!ChannelSubs.TryGetValue(channel, out var handlers)) return false;
                if (handlers.Contains(handler))
                {
                    return true;
                }

                redisSub.Subscribe(channel, handler);
                return handlers.Add(handler);
            }

            var hashSet = new HashSet<Action<RedisChannel, RedisValue>>();

            if (ChannelSubs.TryAdd(channel, hashSet))
            {
                redisSub.Subscribe(channel, handler);
                return hashSet.Add(handler); 
            }

            hashSet = null;

            return false;
        }
        

        public bool UnsubscribeChannel(string channel, Action<RedisChannel, RedisValue> handler)
        {
            if (!ChannelSubs.TryGetValue(channel, out var handlers)) return false;

            if (!handlers.Contains(handler)) return false;

            if (!handlers.Remove(handler)) return false;

            redisSub.Unsubscribe(channel, handler);
            return true;

        }

        public long RightPush(RedisKey key, RedisValue value)
        {
          return Database.ListRightPush(key, value);
        }

        public  RedisValue RightPop(RedisKey key)
        {
            return Database.ListRightPop(key);
        }

        public  long LeftPush(RedisKey key, RedisValue value)
        {
           return Database.ListLeftPush(key, value);
        }

        public  RedisValue LeftPop(RedisKey key)
        {
            return Database.ListLeftPop(key);
        }

        public long Publish(string channel, RedisValue value)
        {
            return Database.Publish(channel, value);
        }

        public IDictionary<string, RedisValue> GetAllCachedData()
        {
            return Cache;
        }

        public bool TryGetCachedData(string key, out RedisValue value)
        {
            return Cache.TryGetValue(key, out value);
        }

        public void AddPartialObserver(IRedisClientObserver observer, params string[] keys)
        {
            Subscribe(Cache.AddPartialObserver(observer, keys).Keys);
        }

        public void AddPartialObserver(Action<RedisChangedEventArgs> action, params string[] keys)
        {
            Subscribe(Cache.AddPartialObserver(action, keys).Keys);
        }

        public void RemovePartialObserver(IRedisClientObserver observer, params string[] keys)
        {
            Unsubscribe(Cache.RemovePartialObserver(observer, keys).Keys);
        }

        public void RemovePartialObserver(IRedisClientObserver observer)
        {
            Unsubscribe(Cache.RemovePartialObserver(observer).Keys);
        }

        public void RemovePartialObserver(params string[] keys)
        {
            foreach (var kv in Cache.RemovePartialObserver(keys)) Unsubscribe(kv.Key, kv.Value.Count);
        }

        public void RemoveAllObservers()
        {
            foreach (var kv in Cache.RemoveAllObservers()) Unsubscribe(kv.Key, kv.Value.Count);
        }

        private readonly List<Task> tasks = new List<Task>();

        protected virtual void UpdateAllData()
        {
            tasks.Clear();
            

            for (int i = 0; i < CachedKeys.Keys.Count; i++)
            {
                var index = i;

                tasks.Add(Task.Run(() =>
                {
                    var key = CachedKeys.Keys.ElementAt(index);
                    Cache.AddOrUpdate(key, Database.StringGet(key));
                }));
            }
            

            foreach (var cacheKey in Cache.Keys)
            {
                if(CachedKeys.ContainsKey(cacheKey)) continue;

                tasks.Add(Task.Run(() => { Cache.TryRemove(cacheKey, out _); }));
            }


            Task.WaitAll(tasks.ToArray());
            
        }

        protected async Task UpdateAllDataForeverAsync()
        {
            while (IsConnected)
            {
                try
                {
                    UpdateAllData();
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
