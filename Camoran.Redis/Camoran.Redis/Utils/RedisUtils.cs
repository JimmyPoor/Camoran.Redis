using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Camoran.Redis.Utils
{
    /// <summary>
    /// RedisBoss class in charge of creating ConnectionMultiplexer object 
    /// 1 Use singleton pattern to create ConnectionMultiplexer
    /// 2 Get IDatabase instance
    /// 3 Bind some events 
    /// </summary>
    public class RedisBoss
    {
        static object _lock = new object();
        static string _redisConnString = "";
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

                    _connectionManger.ErrorMessage += _connectionManger_ErrorMessage;
                    _connectionManger = ConnectionMultiplexer.Connect(_redisConnString);
                }

                return _connectionManger;
            }
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


    public class RedisKey
    {
        public bool KeyIsExist(string key) => RedisBoss.GetDB().KeyExists(key);

        public void KeyPersist(string key) => RedisBoss.GetDB().KeyPersist(key);

        public bool KeyIsExpire(string key, TimeSpan expire) => RedisBoss.GetDB().KeyExpire(key, expire);

        public long KeyIncrement(string key) => RedisBoss.GetDB().StringIncrement(key);

        public byte[] Doump(string key) => RedisBoss.GetDB().KeyDump(key);

        public void KeyReName(string key, string newKey) => RedisBoss.GetDB().KeyRename(key, newKey);
    }


    public class RedisString
    {

        public string Get(string key)
        {
            return RedisBoss.GetDB().StringGet(key);
        }

        public string Get(string key, int db)
        {
            return RedisBoss.GetDB(db).StringGet(key);
        }

        public void Set(string key, string val)
        {
            RedisBoss.GetDB().StringSet(key, val);
        }

        public void Set(string key, string val, TimeSpan expire)
        {
            RedisBoss.GetDB().StringSet(key, val, expire);
        }

        public void Set<T>(string key, T val)
        {
            var valstr = JsonConvert.SerializeObject(val);

            Set(key, valstr);
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
        public void Hset(string hkey, IDictionary<string, string> keyValues)
        {
            var entries = new List<HashEntry>();
            foreach (var item in keyValues)
                entries.Add(new HashEntry(item.Key, item.Value));

            RedisBoss.GetDB().HashSet(hkey, entries.ToArray());
        }

        public string Hget(string hkey, string key) => RedisBoss.GetDB().HashGet(hkey, key);

        public IDictionary<string, string> HGetAll(string hkey)
        {
            var dic = new Dictionary<string, string>();
            var entries = RedisBoss.GetDB().HashGetAll(hkey);
            if (entries == null || entries.Length <= 0) return null;

            foreach (var item in entries.ToDictionary())
                dic.Add(item.Key, item.Value);

            return dic;
        }

        public bool HkeyExists(string hkey) => RedisBoss.GetDB().KeyExists(hkey);

        public List<string> Hkeys(string hkey) => RedisBoss.GetDB().HashKeys(hkey).ConvertToStringList();

        public long Hlen(string hkey) => RedisBoss.GetDB().HashLength(hkey);

        public List<string> Hvals(string hkey) => RedisBoss.GetDB().HashValues(hkey).ConvertToStringList();

    }


    public class RedisList
    {
        public string Lpop(string key) => RedisBoss.GetDB().ListLeftPop(key);

        public void Lpush(string key, string val) => RedisBoss.GetDB().ListLeftPush(key, val);

        public string Rpop(string key) => RedisBoss.GetDB().ListRightPop(key);

        public void Rpush(string key, string val) => RedisBoss.GetDB().ListRightPush(key, val);

        public long Llen(string key) => RedisBoss.GetDB().ListLength(key);

        public List<string> Lrange(string key, int start, int stop) => RedisBoss.GetDB().ListRange(key, start, stop).ConvertToStringList();

        public void Linsert(string key, string preVal, string val) => RedisBoss.GetDB().ListInsertAfter(key, preVal, val);

        public string Lindex(string key, int index) => RedisBoss.GetDB().ListGetByIndex(key, index);

        /// <summary>
        /// Remove val in list
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="val">val</param>
        /// <param name="count"></param>
        /// <returns></returns>
        public long Lremove(string key, string val, int count) => RedisBoss.GetDB().ListRemove(key, val, count);
    }


    public class RedisSet
    {
        public void Sadd(string key, string val) => RedisBoss.GetDB().SetAdd(key, val);

        public long Scard(string key) => RedisBoss.GetDB().SetLength(key);

        public void Spop(string key) => RedisBoss.GetDB().SetPop(key);

        public void Smove(string sourceKey, string destKey, string val) => RedisBoss.GetDB().SetMove(sourceKey, destKey, val);

        public bool Srem(string key, string val) => RedisBoss.GetDB().SetRemove(key, val);

        public List<string> Smember(string key) => RedisBoss.GetDB().SetMembers(key).ConvertToStringList();

        public List<string> Sdiff(string first, string second) =>
             RedisBoss.GetDB().SetCombine(SetOperation.Difference, first, second).ConvertToStringList();

        public List<string> Sunion(string first, string second) =>
            RedisBoss.GetDB().SetCombine(SetOperation.Union, first, second).ConvertToStringList();

        public List<string> Sinter(string first, string second) =>
            RedisBoss.GetDB().SetCombine(SetOperation.Intersect, first, second).ConvertToStringList();
    }


    public class RedisSortSet
    {
    }
}
