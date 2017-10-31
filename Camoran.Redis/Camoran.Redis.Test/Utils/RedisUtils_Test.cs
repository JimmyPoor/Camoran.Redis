﻿using Camoran.Redis.Utils;
using StackExchange.Redis;
using System;
using System.Threading;
using Xunit;

namespace Camoran.Redis.Test
{

    public class RedisUtils_Test
    {
        string conn = @"localhost";
        string defaultKey = "abc";
        string defaultValue = "bcd";
        TimeSpan defaultExpire = new TimeSpan(0, 0, 0, 0, 10);
        IDatabase _db;
        RedisKeys rk = new RedisKeys();
        RedisString rs = new RedisString();
        RedisHash hs = new RedisHash();
        RedisList rl = new RedisList();
        RedisSet rst = new RedisSet();


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


        #region [ Redis String ]

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

        #endregion


        #region [ Redis List ]

        [Fact]
        public void List_Push_Pop_Test()
        {
            rl.Lrem(defaultKey, defaultValue, 0);
            var val = rl.Lpop(defaultKey);
            var pushVal = rl.Lpush(defaultKey, defaultValue);
            var popVal = rl.Lpop(defaultKey);

            var temp = "temp";
            for (int i = 0; i <= 10; i++)
                rl.Rpush(temp, i.ToString());

            var rpopVal = rl.Rpop(temp);

            Assert.Null(val);
            Assert.True(pushVal > 0);
            Assert.Equal(popVal, defaultValue);
            Assert.Equal(rpopVal, "10");
        }

        [Fact]
        public void List_Range_Test()
        {
            var key = "ddd";
            int start = 0, end = 10;

            for (int i = start; i <= end; i++)
                rl.Lrem(key, i.ToString(), 0);

            for (int i = start; i <= end; i++)
                rl.Rpush(key, i.ToString());

            var all = rl.Lrange(key, 0, -1);
            var range = rl.Lrange(key, 0, 2);

            Assert.Equal(all.Count, end + 1);
            Assert.Equal(range.Count, 3);

        }

        [Fact]
        public void List_Insert_Test()
        {
            var tempKey = "temp";
            var prev = "prev";
            var next = "next";

            rl.Lpush(tempKey, prev);
            rl.Linsert(tempKey, prev, next);

            var realPrev = rl.Lindex(tempKey, 0);
            var realNext = rl.Lindex(tempKey, 1);

            Assert.Equal(prev, realPrev);
            Assert.Equal(next, realNext);
        }

        #endregion


        #region [ Redis Set ]

        [Fact]
        public void Set_Add_Test()
        {
            rst.Sadd(defaultKey, defaultValue);
            rst.Sadd(defaultKey,defaultValue);

            var vals = rst.Smember(defaultKey);
            var count = rst.Scard(defaultKey);

            Assert.Equal(vals.Count, count);
        }

        [Fact]
        public void SMove_Test()
        {
           //rst.Smove()
        }

        #endregion


        #region [ Redis SortSet ]

        #endregion


        #region [ Redis Lock ]

        #endregion


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
