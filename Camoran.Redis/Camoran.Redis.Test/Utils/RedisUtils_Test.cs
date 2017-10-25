using Camoran.Redis.Utils;
using StackExchange.Redis;
using System;
using System.Threading;
using Xunit;

namespace Camoran.Redis.Test
{
    public class RedisUtils_Test
    {
        string conn = @"localhost";
        RedisKeys rk = new RedisKeys();
        RedisString rs = new RedisString();
        string defaultKey = "0";
        string defaultValue = "defaultValue";
        TimeSpan defaultExpire = new TimeSpan(0, 0, 0, 0, 10);
        IDatabase _db;

        public RedisUtils_Test()
        {
            _db = RedisBoss.GetDB();
        }

        [Fact]
        public void Create_ConnectionManager_Test()
        {
            Assert.NotNull(GetManager());
        }

        [Fact]
        public void Connect_Test()
        {
            var cm = GetManager();
            Assert.True(cm.IsConnected);
        }

        [Fact]
        public void RedisKey_Key_Exists_Test()
        {
            rs.Set(defaultKey, defaultValue);

            Assert.True(rk.Exists(defaultKey));
        }

        [Fact]
        public void RedisKey_Key_Expire_Test()
        {
            rs.Set(defaultKey, defaultValue);

            rk.Expire(defaultKey, defaultExpire);

            Thread.Sleep(1000);

            Assert.True(!rk.Exists(defaultKey));
        }

        [Fact]
        public void RedisKey_Key_Persist_Test()
        {
            rs.Set(defaultKey, defaultValue);

            rk.Expire(defaultKey, defaultExpire);

            rk.Persist(defaultKey);

            Assert.True(rk.Exists(defaultKey));
        }

        [Fact]
        public void Redis_Key_Increment_Test()
        {
            rs.Set(defaultKey, defaultValue);

            var current = rk.Increment(defaultKey, 10);

            Assert.Equal(current % 10, 0);
        }

        [Fact]
        public void Redis_Value_Dump_Test()
        {
            rs.Set(defaultKey, defaultValue);

            var val = rk.Doump(defaultKey);

            Assert.IsType<byte[]>(val);
        }

        [Fact]
        public void Redis_Value_Restore_Test()
        {
            rs.Set(defaultKey, defaultValue);

            var val = rk.Doump(defaultKey);

            rk.Expire(defaultKey, defaultExpire);
            Thread.Sleep(1000);
            rk.Restore(defaultKey,val,null);

            var v = rs.Get(defaultKey);

            Assert.Equal(v, defaultValue);
        }


        private ConnectionMultiplexer GetManager()
        {
            RedisBoss.SetConnection(conn);

            return RedisBoss.ConnectionManger;
        }
    }
}
