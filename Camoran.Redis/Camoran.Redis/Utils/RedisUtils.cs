using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Camoran.Redis.Utils
{
    /// <summary>
    /// RedisBoss class in charge of creating ConnectionMultiplexer object 
    /// 1 Using singleton pattern to create ConnectionMultiplexer
    /// 2 Get IDatabase instance
    /// 3 Bind some events 
    /// </summary>
    public class RedisBoss
    {
        static object _lock = new object();
        static string _redisConnString = "localhost";
        static ConnectionMultiplexer _connectionManger;
        public static ConnectionMultiplexer ConnectionManger
        {
            get
            {
                if (_connectionManger == null)
                {
                    lock (_lock)
                    {
                        if (_connectionManger != null)
                            return _connectionManger;
                    }

                    _connectionManger = ConnectionMultiplexer.Connect(_redisConnString);
                    _connectionManger.ErrorMessage += _connectionManger_ErrorMessage;
                }
                return _connectionManger;
            }
        }

        public static void SetConnection(string conn)
        {
            _redisConnString = conn;
        }


        public static IDatabase GetDB(int db = -1)
        {
            return ConnectionManger.GetDatabase(db);
        }

        public static void Dispose()
        {
            if (_connectionManger != null)
                _connectionManger.Dispose();
        }

        public static Action<RedisErrorEventArgs> WhenError { get; set; }

        private static void _connectionManger_ErrorMessage(object sender, RedisErrorEventArgs e)
        {
            if (WhenError == null)
                Console.WriteLine($"Error:{e.Message}");
            else
                WhenError(e);
        }
    }


    public static class RedisValueExtension
    {
        public static List<T> ConvertToObjects<T>(this RedisValue[] rvs)
            where T : class
        {
            var vals = new List<T>();
            if (rvs != null)
                for (int i = 0; i < rvs.Length; i++)
                    vals.Add(
                       JsonConvert.DeserializeObject<T>(rvs[i]));

            return vals;
        }


        public static List<string> ConvertToStringList(this RedisValue[] rvs)
        {
            var vals = new List<string>();
            if (rvs != null)
                for (int i = 0; i < rvs.Length; i++)
                    vals.Add(rvs[i]);

            return vals;
        }
    }


    public class RedisKeys
    {
        private IDatabase _db;

        public RedisKeys(int db = -1)
        {
            _db = RedisBoss.GetDB(db);
        }

        public bool Exists(string key) => _db.KeyExists(key);

        public void Persist(string key) => _db.KeyPersist(key);

        public bool Expire(string key, TimeSpan expire) => _db.KeyExpire(key, expire);

        public long Increment(string key, long val) => _db.StringIncrement(key, val);

        public byte[] Doump(string key) => _db.KeyDump(key);

        public void ReName(string key, string newKey) => _db.KeyRename(key, newKey);

        public void Restore(string key, byte[] val, TimeSpan? expiry) => _db.KeyRestore(key, val, expiry);

        public TimeSpan? TTL(string key) => _db.KeyTimeToLive(key);

        public bool Del(string key) => _db.KeyDelete(key);

        //public bool SetNx(string key,string val) => _db.KeyExists(key);
    }


    public class RedisString
    {
        private IDatabase _db;

        public RedisString(int db = -1)
        {
            _db = RedisBoss.GetDB(db);
        }

        public string Get(string key) => _db.StringGet(key);

        public string Get(string key, int db) => _db.StringGet(key);

        public bool Set(string key, string val) => _db.StringSet(key, val);

        public bool Set(string key, string val, TimeSpan? expire) => _db.StringSet(key, val, expire);

        public string GetSet(string key, string val) => _db.StringGetSet(key, val);

        public string GetSet<T>(string key, T val) => _db.StringGetSet(key, JsonConvert.SerializeObject(val));

        public bool Set<T>(string key, T val) => Set(key, val, null);

        public bool Set<T>(string key, T val, TimeSpan? expire) => Set(key, JsonConvert.SerializeObject(val), expire);

        public bool SetNx(string key, string val)
        {
            if (_db.KeyExists(key)) return false;

            return Set(key, val);
        }

        public T Get<T>(string key)
        {
            var valstr = Get(key);

            if (string.IsNullOrEmpty(valstr))
                return default(T);

            return JsonConvert.DeserializeObject<T>(valstr);

        }

    }


    public class RedisHash
    {
        private IDatabase _db;

        public RedisHash(int db = -1)
        {
            _db = RedisBoss.GetDB(db);
        }

        public void Hset(string hkey, Dictionary<string, string> keyValues)
        {
            var entries = new List<HashEntry>();
            foreach (var item in keyValues)
                entries.Add(new HashEntry(item.Key, item.Value));

            _db.HashSet(hkey, entries.ToArray());
        }

        public bool Hset<T>(string hkey, string key, T val) => _db.HashSet(hkey, key, JsonConvert.SerializeObject(val));

        public bool Hset(string hkey, string key, string val) => _db.HashSet(hkey, key, val);

        public string Hget(string hkey, string key) => _db.HashGet(hkey, key);

        public T Hget<T>(string hkey, string key)
        {
            var val = _db.HashGet(hkey, key);
            if (!string.IsNullOrEmpty(val))
                return JsonConvert.DeserializeObject<T>(val);

            return default(T);
        }

        public Dictionary<string, string> HGetAll(string hkey)
        {
            var dic = new Dictionary<string, string>();
            var entries = RedisBoss.GetDB().HashGetAll(hkey);
            if (entries == null || entries.Length <= 0) return null;

            foreach (var item in entries.ToDictionary())
                dic.Add(item.Key, item.Value);

            return dic;
        }

        public bool HkeyExists(string hkey) => _db.KeyExists(hkey);

        public List<string> Hkeys(string hkey) => _db.HashKeys(hkey).ConvertToStringList();

        public long Hlen(string hkey) => _db.HashLength(hkey);

        public List<string> Hvals(string hkey) => _db.HashValues(hkey).ConvertToStringList();

    }


    public class RedisList
    {
        private IDatabase _db;

        public RedisList(int db = -1)
        {
            _db = RedisBoss.GetDB(db);
        }

        public string Lpop(string key) => _db.ListLeftPop(key);

        public long Lpush(string key, string val) => _db.ListLeftPush(key, val);

        public string Rpop(string key) => _db.ListRightPop(key);

        public long Rpush(string key, string val) => _db.ListRightPush(key, val);

        public long Llen(string key) => RedisBoss.GetDB().ListLength(key);

        public List<string> Lrange(string key, int start, int stop) => _db.ListRange(key, start, stop).ConvertToStringList();

        public long Linsert(string key, string preVal, string val) => _db.ListInsertAfter(key, preVal, val);

        public string Lindex(string key, int index) => _db.ListGetByIndex(key, index);

        public long Lrem(string key, string val, int count) => _db.ListRemove(key, val, count);
    }   


    public class RedisSet
    {
        public bool Sadd(string key, string val) => RedisBoss.GetDB().SetAdd(key, val);

        public long Scard(string key) => RedisBoss.GetDB().SetLength(key);

        public string Spop(string key) => RedisBoss.GetDB().SetPop(key);

        public bool Smove(string sourceKey, string destKey, string val) => RedisBoss.GetDB().SetMove(sourceKey, destKey, val);

        public bool Srem(string key, string val) => RedisBoss.GetDB().SetRemove(key, val);

        public List<string> Smember(string key) => RedisBoss.GetDB().SetMembers(key).ConvertToStringList();

        public List<string> Sdiff(string first, string second) =>
             RedisBoss.GetDB().SetCombine(SetOperation.Difference, first, second).ConvertToStringList();

        public List<string> Sunion(string first, string second) =>
            RedisBoss.GetDB().SetCombine(SetOperation.Union, first, second).ConvertToStringList();

        public List<string> Sinter(string first, string second) =>
            RedisBoss.GetDB().SetCombine(SetOperation.Intersect, first, second).ConvertToStringList();
    }


    public class RedisSortedSet
    {
        public bool Zadd(string key, string val, double score) => RedisBoss.GetDB().SortedSetAdd(key, val, score);

        public long Zcount(string key) => RedisBoss.GetDB().SortedSetLength(key);

        public List<string> ZrangeByScore(string key, long start, long end = -1) => RedisBoss.GetDB().SortedSetRangeByScore(key, start, end).ConvertToStringList();

        public List<string> ZrangeByRank(string key, long start, long end) => RedisBoss.GetDB().SortedSetRangeByRank(key, start, end).ConvertToStringList();

        public bool Zrem(string key, string val) => RedisBoss.GetDB().SortedSetRemove(key, val);

        public long ZremRangeByRank(string key, long start, long end) => RedisBoss.GetDB().SortedSetRemoveRangeByRank(key, start, end);

        public long ZremRangeByScore(string key, long start, long end) => RedisBoss.GetDB().SortedSetRemoveRangeByScore(key, start, end);

        public void ZScore(string key, string val) => RedisBoss.GetDB().SortedSetScore(key, val);
    }


    public class RedisLock
    {
        RedisKeys _redisKeys;
        RedisString _redisString;
        TimeSpan _lockExpireTime;
        static RedisKey _lockKey = "lockObj";

        public RedisLock(RedisKeys keys, RedisString redisString, TimeSpan lockExpireTime)
        {
            if (keys == null || redisString == null)
                throw new ArgumentException("params is not allowed null");

            _redisKeys = keys;
            _redisString = redisString;
            _lockExpireTime = lockExpireTime;
        }

        public bool GetLock()
        {
            var currentStamp = ConvertToTimestamp(DateTime.Now + _lockExpireTime);
            if (!GetLock(currentStamp)) // try to get lock
            {
                var timestamp = Convert.ToInt64(_redisString.Get(_lockKey));
                if (timestamp <= currentStamp) // if lock has been expired
                {
                    var oldStamp = Convert.ToInt64(_redisString.GetSet(_lockKey, currentStamp)); // get old timestamp to check whether another thread may get this lock

                    return oldStamp == timestamp || oldStamp == 0; // if no another thread got lock then current thread got this lock instead,oldStamp is zero mean  lock key been deleted
                }

                return false;
            }

            return true;
        }


        public bool DelLock()
        {
            return _redisKeys.Del(_lockKey);
        }


        protected bool GetLock(long timestamp)
        {
            return _redisString.SetNx(_lockKey, timestamp.ToString());
        }


        protected long ConvertToTimestamp(DateTime dt)
        {
            var initial = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var ticks = dt.AddHours(-8) - initial;

            return (long)ticks.TotalMilliseconds;
        }

        protected DateTime ConvertToDateTime(long timestamp)
        {
            var initial = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return initial.AddMilliseconds(timestamp).AddDays(8);
        }

    }
}
