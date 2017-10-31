using Camoran.Redis.Utils;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace Camoran.Redis.PubSub
{

    public interface IRedisSubPub
    {
        void Publish<T>(string channel, T message);
        void Subscribe<T>(string channel, Action<string, T> cb);
        Task SubscribeAsync<T>(string channel, Action<string, T> cb);
    }


    public class RedisSubPub : RedisBoss, IRedisSubPub
    {
        ISubscriber _subscriber;

        public RedisSubPub()
        {
            _subscriber = ConnectionManger.GetSubscriber();
        }

        public void Publish<T>(string channel, T message)
        {
            var msg = JsonConvert.SerializeObject(message);
            _subscriber.Publish(channel, msg);
        }

        public void Subscribe<T>(string channel, Action<string, T> cb)
        {
            _subscriber.Subscribe(channel, (c, m) =>
            {
                var message = JsonConvert.DeserializeObject<T>(m);
                cb(c, message);
            });
        }

        public Task SubscribeAsync<T>(string channel, Action<string, T> cb)
        {
            return _subscriber.SubscribeAsync(channel, (c, m) =>
             {
                 var message = JsonConvert.DeserializeObject<T>(m);
                 cb(c, message);
             });
        }
    }

}
