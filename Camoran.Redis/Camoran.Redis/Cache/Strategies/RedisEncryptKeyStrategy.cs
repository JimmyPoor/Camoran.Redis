using Camoran.Redis.Utils;
using System;
using System.Collections.Generic;
using System.Text;
using System.Security.Cryptography;

namespace Camoran.Redis.Cache
{

    public class RedisEncrypt
    {
        protected EncryptType _entryptType;
        protected Encoding _encoding = Encoding.UTF8;
        protected HMACMD5 _md5;

        public RedisEncrypt(Encoding encoding, EncryptType entryptType)
        {
            if (encoding != null)
                _encoding = encoding;

            _entryptType = entryptType;
            _md5 = new HMACMD5();
        }

        protected string MD5Encrypt(string key)
        {
            var buffer = _encoding.GetBytes(key.ToString());
            var encryptKeyBytes = _md5.ComputeHash(buffer);
            var encryptKey = Encoding.ASCII.GetString(encryptKeyBytes);

            return encryptKey;
        }

        protected string SHAEncrypt(string key)
        {
            throw new RedisCacheException();
        }


        public enum EncryptType
        {
            MD5 = 1
        }
    }


    public class RedisEncryptKeyStrategy<Value> : RedisEncrypt, ICahceStrategy<string, Value>
    {
        RedisString _redisString;
        RedisKeys _redisKey;

        public RedisEncryptKeyStrategy(Encoding encoding, EncryptType entryptType = EncryptType.MD5)
            : base(encoding, entryptType)
        {
        }

        public Value Get(string key)
        {
            var enctryptKey = MD5Encrypt(key);

            return _redisString.Get<Value>(enctryptKey);
        }

        public void Set(string key, Value val, TimeSpan expireTime)
        {
            if (val == null) return;
            if (_entryptType == EncryptType.MD5)
            {
                var enctryptKey = MD5Encrypt(key);

                _redisString.Set(key, val);

                SetExpire(key, expireTime);
            }
        }

        public bool Remove(string key)
        {
            return _redisKey.Del(key);
        }

        public void SetExpire(string key, TimeSpan expireTime)
        {
            _redisKey.Expire(key, expireTime);
        }
    }


    public class RedisEncryptKeyWithSetStrategy : RedisEncrypt, ICahceStrategy<string, List<string>>
    {
        RedisSet _set;
        RedisKeys _key;

        public RedisEncryptKeyWithSetStrategy(Encoding encoding, EncryptType entryptType = EncryptType.MD5)
            : base(encoding, entryptType)
        {
            _set = new RedisSet();
            _key = new RedisKeys();
        }

        public List<string> Get(string key)
        {
            var enctryptKey = MD5Encrypt(key);

            return _set.Smember(enctryptKey);
        }

        public void Set(string key, List<string> val, TimeSpan expireTime)
        {
            if (val == null) return;
            if (_entryptType == EncryptType.MD5)
            {
                var enctryptKey = MD5Encrypt(key);

                foreach (var i in val)
                    _set.Sadd(enctryptKey, i); // redis set will remove duplicate values

                SetExpire(key, expireTime);
            }
        }

        public bool Remove(string key)
        {
            return _key.Del(key);
        }

        public void SetExpire(string key, TimeSpan expireTime)
        {
            _key.Expire(key, expireTime);
        }

    }


    public class RedisEncryptKeyWithHashStrategy : RedisEncrypt, ICahceStrategy<string, Dictionary<string, string>>
    {
        RedisHash _hash;
        RedisKeys _key;

        public RedisEncryptKeyWithHashStrategy(Encoding encoding, EncryptType entryptType = EncryptType.MD5)
            : base(encoding, entryptType)
        {
            _hash = new RedisHash();
            _key = new RedisKeys();
        }

        public Dictionary<string, string> Get(string key)
            => _hash.HGetAll(MD5Encrypt(key));


        public bool Remove(string key)
        {
            return _key.Del(key);
        }

        public void Set(string key, Dictionary<string, string> val, TimeSpan expireTime)
        {
            if (val == null) return;
            if (_entryptType == EncryptType.MD5)
            {
                var enctryptKey = MD5Encrypt(key);
                _hash.Hset(enctryptKey, val);
                SetExpire(enctryptKey, expireTime);
            }
        }

        public void SetExpire(string key, TimeSpan expireTime)
            => _key.Expire(key, expireTime);
    }

}

