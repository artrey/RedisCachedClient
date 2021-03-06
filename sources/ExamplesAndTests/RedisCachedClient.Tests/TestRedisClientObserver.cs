using ConcurrentObservableCollections.ConcurrentObservableDictionary;
using StackExchange.Redis;

namespace RedisCachedClient.Tests
{
    public class TestRedisClientObserver : IRedisClientObserver
    {
        public bool Occured { get; private set; } = false;
        public string Key { get; }
        public RedisValue Value { get; }

        public TestRedisClientObserver(string key, RedisValue value)
        {
            Key = key;
            Value = value;
        }
        
        public void OnEventOccur(DictionaryChangedEventArgs<string, RedisValue> args)
        {
            Occured = Key == args.Key && Value == args.NewValue;
        }
    }
}
