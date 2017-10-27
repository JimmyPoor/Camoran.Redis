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
        string defaultKey = "key";
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


        #region [ Redis key ]

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
            rk.Restore(defaultKey, val, null);

            var v = rs.Get(defaultKey);

            Assert.Equal(v, defaultValue);
        }

        #endregion

        [Fact]
        public void RedisString_Set_Test()
        {
            rs.Set(defaultKey, defaultValue);
            var val = rs.Get(defaultKey);
            Assert.Equal(val, defaultValue);

            var newVal = "newVal";
            rs.Set(defaultKey, newVal);
            val = rs.Get(defaultKey);
            Assert.NotEqual(val, defaultValue);
            Assert.Equal(val, newVal);

            rs.Set(defaultKey, null);
            val = rs.Get(defaultKey);
            Assert.Equal(val, null);
        }

        [Fact]
        public void RedisString_Set_Object_With_Different_Reference()
        {
            var o = new Demo { Val = defaultValue };
            rs.Set(defaultKey, o);
            var o2 = rs.Get<Demo>(defaultKey);

            Assert.NotEqual(o, o2);
            Assert.Equal(o.Val, o2.Val);
        }



        private ConnectionMultiplexer GetManager()
        {
            RedisBoss.SetConnection(conn);

            return RedisBoss.ConnectionManger;
        }


        class Demo
        {
            public string Val { get; set; }
        }

    }
}
