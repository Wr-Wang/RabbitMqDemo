using RabbitMqDemo.Entities;
using RabbitMqDemo.Interfaces;
using StackExchange.Redis;
using System.Text.Json;

namespace RabbitMqDemo.Services
{
    public class RedisIdempotentService : IMsgIdempotentService
    {
        private readonly IDatabase _redisDb;
        private const string KeyPrefix = "mq:idempotent:";

        // 正确构造函数，依赖 IConnectionMultiplexer
        public RedisIdempotentService(IConnectionMultiplexer multiplexer)
        {
            _redisDb = multiplexer.GetDatabase();
        }

        public async Task<bool> TryLockMsgAsync(string msgId, int expireSecond)
        {
            var key = $"{KeyPrefix}{msgId}";
            // 原子SETNX加过期时间，防止并发重复消费
            var record = new IdempotentRecord
            {
                Finished = false,
                ExpireTs = DateTimeOffset.UtcNow.AddSeconds(expireSecond).ToUnixTimeMilliseconds()
            };
            string json = JsonSerializer.Serialize(record);
            var ok = await _redisDb.StringSetAsync(key, json, TimeSpan.FromSeconds(expireSecond), When.NotExists);
            return ok;
        }

        public async Task MarkFinishAsync(string msgId)
        {
            var key = $"{KeyPrefix}{msgId}";
            RedisValue val = await _redisDb.StringGetAsync(key);
            if (!val.HasValue) return;

            // 显式转string，消除JsonSerializer.Deserialize重载二义性
            string jsonStr = val.ToString();
            var record = JsonSerializer.Deserialize<IdempotentRecord>(jsonStr)!;
            if (record == null) return;

            record.Finished = true;
            string updateJson = JsonSerializer.Serialize(record);
            await _redisDb.StringSetAsync(key, updateJson);
        }

        public async Task UnLockMsgAsync(string msgId)
        {
            var key = $"{KeyPrefix}{msgId}";
            await _redisDb.KeyDeleteAsync(key);
        }
    }
}
