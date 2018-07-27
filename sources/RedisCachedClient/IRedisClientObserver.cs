using StackExchange.Redis;
using ConcurrentObservableCollections.ConcurrentObservableDictionary;

namespace RedisCachedClient
{
    public interface IRedisClientObserver : IDictionaryObserver<string, RedisValue>
    {
    }
}
