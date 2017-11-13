using System;
using System.Collections.Generic;
using System.Text;

namespace Camoran.Redis.Cache
{

    public interface IRedisCahceStrategy<Key, Value>
    {
        void Set(Key key, Value val, TimeSpan? expireTime);
        Value Get(Key key);
        bool Remove(Key key);
        void SetExpire(Key key, TimeSpan expireTime);
        void SetConnection(string conn);
    }


    public class RedisCache<Key, Value>
    {
        IRedisCahceStrategy<Key, Value> _strategy;

        public RedisCache(IRedisCahceStrategy<Key, Value> strategy) : this(strategy, null) { }

        public RedisCache(IRedisCahceStrategy<Key, Value> strategy, string conn)
        {
            this._strategy = strategy ?? throw new RedisCacheException(ErrorInfo.STRATEGY_NOT_ALLOWED_NULL);

            strategy.SetConnection(conn);
        }

        public virtual Value Get(Key key)
        {
            return _strategy.Get(key);
        }

        public virtual bool Remove(Key key)
        {
            return _strategy.Remove(key);
        }

        public virtual void Set(Key key, Value val, TimeSpan expireTime)
        {
            _strategy.Set(key, val, expireTime);
        }

        public virtual void SetExpire(Key key, TimeSpan expireTime)
        {
            _strategy.SetExpire(key, expireTime);
        }
    }


    [Serializable]
    internal class RedisCacheException : Exception
    {
        private string _message;

        public RedisCacheException() { }

        public RedisCacheException(string message) : base(message) => _message = message;

        public RedisCacheException(string message, Exception inner)
            : base(message, inner)
        { }
    }


    internal static class ErrorInfo
    {
        internal static string STRATEGY_NOT_ALLOWED_NULL = "cache strategy is not allowed null";
        internal static string CONN_NOT_ALLOWED_EMPTY = "connect string not allowd be null";
    }

}
