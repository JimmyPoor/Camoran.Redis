﻿using Newtonsoft.Json;
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
            Dispose();
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
            _connectionManger = null;
        }

        public static Action<RedisErrorEventArgs> WhenError { get; set; }

        public static string Covnert<T>(T val) => JsonConvert.SerializeObject(val);

        public static T CovnertToObj<T>(string val) => JsonConvert.DeserializeObject<T>(val);


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

        public bool Exists(string key, int db = -1) => RedisBoss.GetDB(db).KeyExists(key);

        public void Persist(string key, int db = -1) => RedisBoss.GetDB(db).KeyPersist(key);

        public bool Expire(string key, TimeSpan expire, int db = -1) => RedisBoss.GetDB(db).KeyExpire(key, expire);

        public long Increment(string key, long val, int db = -1) => RedisBoss.GetDB(db).StringIncrement(key, val);

        public byte[] Doump(string key, int db = -1) => RedisBoss.GetDB(db).KeyDump(key);

        public void ReName(string key, string newKey, int db = -1) => RedisBoss.GetDB(db).KeyRename(key, newKey);

        public void Restore(string key, byte[] val, TimeSpan? expiry, int db = -1) => RedisBoss.GetDB(db).KeyRestore(key, val, expiry);

        public TimeSpan? TTL(string key, int db = -1) => RedisBoss.GetDB(db).KeyTimeToLive(key);

        public bool Del(string key, int db = -1) => RedisBoss.GetDB(db).KeyDelete(key);

    }


    public class RedisString
    {

        public string Get(string key, int db = -1) => RedisBoss.GetDB(db).StringGet(key);

        public bool Set(string key, string val, int db = -1) => RedisBoss.GetDB(db).StringSet(key, val);

        public bool Set(string key, string val, TimeSpan? expire, int db = -1) => RedisBoss.GetDB(db).StringSet(key, val, expire);

        public string GetSet(string key, string val, int db = -1) => RedisBoss.GetDB(db).StringGetSet(key, val);

        public string GetSet<T>(string key, T val, int db = -1) => RedisBoss.GetDB(db).StringGetSet(key, JsonConvert.SerializeObject(val));

        public bool Set<T>(string key, T val, int db = -1) => Set(key, val, null);

        public bool Set<T>(string key, T val, TimeSpan? expire, int db = -1) => Set(key, JsonConvert.SerializeObject(val), expire, db);

        public bool SetNx(string key, string val, int db = -1)
        {
            return RedisBoss.GetDB(db).StringSet(key, val, null, When.NotExists);
        }

        public T Get<T>(string key, int db = -1)
        {
            var valstr = Get(key, db);

            if (string.IsNullOrEmpty(valstr))
                return default(T);

            return JsonConvert.DeserializeObject<T>(valstr);

        }

    }


    public class RedisHash
    {

        public void Hset<T>(string hkey, Dictionary<string, T> keyValues, int db = -1)
        {
            var entries = new List<HashEntry>();
            try
            {
                foreach (var item in keyValues)
                    Hset(hkey, item.Key, item.Value);
            }
            catch
            {
                RedisBoss.GetDB(db).KeyDelete(hkey);
            }
        }

        public bool Hset<T>(string hkey, string key, T val, int db = -1) => RedisBoss.GetDB(db).HashSet(hkey, key, JsonConvert.SerializeObject(val));

        public bool Hset(string hkey, string key, string val, int db = -1) => RedisBoss.GetDB(db).HashSet(hkey, key, val);

        public string Hget(string hkey, string key, int db = -1) => RedisBoss.GetDB(db).HashGet(hkey, key);

        public T Hget<T>(string hkey, string key, int db = -1)
        {
            var val = RedisBoss.GetDB(db).HashGet(hkey, key);
            if (!string.IsNullOrEmpty(val))
                return JsonConvert.DeserializeObject<T>(val);

            return default(T);
        }

        public Dictionary<string, T> HGet<T>(string hkey, int db = -1)
        {
            var dic = new Dictionary<string, T>();
            var entries = RedisBoss.GetDB().HashGetAll(hkey);
            if (entries == null || entries.Length <= 0) return null;

            foreach (var item in entries.ToDictionary())
                dic.Add(item.Key, RedisBoss.CovnertToObj<T>(item.Value));

            return dic;
        }

        public bool HkeyExists(string hkey, int db = -1) => RedisBoss.GetDB(db).KeyExists(hkey);

        public List<string> Hkeys(string hkey, int db = -1) => RedisBoss.GetDB(db).HashKeys(hkey).ConvertToStringList();

        public long Hlen(string hkey, int db = -1) => RedisBoss.GetDB(db).HashLength(hkey);

        public List<T> Hvals<T>(string hkey, int db = -1) => RedisBoss.GetDB(db).HashValues(hkey).ConvertToObjects<T>();

    }


    public class RedisList
    {

        public T Lpop<T>(string key, int db = -1)
            =>RedisBoss.CovnertToObj<T>(RedisBoss.GetDB(db).ListLeftPop(key));

        public long Lpush<T>(string key, T val, int db = -1)
            => RedisBoss.GetDB(db).ListLeftPush(key,
                RedisBoss.Covnert(val));

        public T Rpop<T>(string key, int db = -1) 
            => RedisBoss.CovnertToObj<T>(RedisBoss.GetDB(db).ListRightPop(key));

        public long Rpush<T>(string key, T val, int db = -1) => RedisBoss.GetDB(db).ListRightPush(key, RedisBoss.Covnert(val));

        public long Llen(string key, int db = -1) => RedisBoss.GetDB().ListLength(key);

        public List<T> Lrange<T>(string key, int start, int stop, int db = -1)
            => RedisBoss.GetDB(db).ListRange(key, start, stop).ConvertToObjects<T>();

        public long Linsert<T>(string key, T preVal, T val, int db = -1)
            => RedisBoss.GetDB(db).ListInsertAfter(key, RedisBoss.Covnert(preVal), RedisBoss.Covnert(val));

        public string Lindex(string key, int index, int db = -1) => RedisBoss.GetDB(db).ListGetByIndex(key, index);

        public long Lrem<T>(string key, T val, int count, int db = -1) 
            => RedisBoss.GetDB(db).ListRemove(key, RedisBoss.Covnert(val), count);

    }


    public class RedisSet
    {

        public bool Sadd<T>(string key, T val, int db = -1) 
            => RedisBoss.GetDB(db).SetAdd(key,RedisBoss.Covnert(val));

        public long Scard(string key, int db = -1) => RedisBoss.GetDB(db).SetLength(key);

        public T Spop<T>(string key, int db = -1) 
            => RedisBoss.CovnertToObj<T>(RedisBoss.GetDB(db).SetPop(key));

        public bool Smove(string sourceKey, string destKey, string val, int db = -1) => RedisBoss.GetDB(db).SetMove(sourceKey, destKey, val);

        public bool Srem<T>(string key, T val, int db = -1) => RedisBoss.GetDB(db).SetRemove(key, RedisBoss.Covnert(val));

        public List<T> Smember<T>(string key, int db = -1) => RedisBoss.GetDB(db).SetMembers(key).ConvertToObjects<T>();

        public List<T> Sdiff<T>(string first, string second, int db = -1) =>
             RedisBoss.GetDB(db).SetCombine(SetOperation.Difference, first, second).ConvertToObjects<T>();

        public List<T> Sunion<T>(string first, string second, int db = -1) =>
            RedisBoss.GetDB(db).SetCombine(SetOperation.Union, first, second).ConvertToObjects<T>();

        public List<T> Sinter<T>(string first, string second, int db = -1) =>
            RedisBoss.GetDB(db).SetCombine(SetOperation.Intersect, first, second).ConvertToObjects<T>();

    }


    public class RedisSortedSet
    {

        public bool Zadd<T>(string key, T val, double score, int db = -1)
            => RedisBoss.GetDB(db).SortedSetAdd(key,RedisBoss.Covnert(val), score);

        public long Zcount(string key, int db = -1) => RedisBoss.GetDB(db).SortedSetLength(key);

        public List<T> ZrangeByScore<T>(string key, long start, long end = -1, int db = -1) => RedisBoss.GetDB(db).SortedSetRangeByScore(key, start, end).ConvertToObjects<T>();

        public List<T> ZrangeByRank<T>(string key, long start, long end, int db = -1) => RedisBoss.GetDB(db).SortedSetRangeByRank(key, start, end).ConvertToObjects<T>();

        public bool Zrem<T>(string key, T val, int db = -1)
            => RedisBoss.GetDB().SortedSetRemove(key,RedisBoss.Covnert(val));

        public long ZremRangeByRank(string key, long start, long end, int db = -1) => RedisBoss.GetDB(db).SortedSetRemoveRangeByRank(key, start, end);

        public long ZremRangeByScore(string key, long start, long end, int db = -1) => RedisBoss.GetDB(db).SortedSetRemoveRangeByScore(key, start, end);

        public double? ZScore(string key, string val, int db = -1) => RedisBoss.GetDB(db).SortedSetScore(key, val);

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
            var nextStamp = ConvertToTimestamp(DateTime.Now + _lockExpireTime);
            var currentStamp = ConvertToTimestamp(DateTime.Now);
            if (!GetLock(nextStamp)) // try to get lock
            {
                var timestamp = Convert.ToInt64(_redisString.Get(_lockKey));
                if (currentStamp > timestamp) // if lock has been expired
                {
                    var oldStamp = Convert.ToInt64(_redisString.GetSet(_lockKey, nextStamp)); // get old timestamp to check whether another thread may get this lock

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
