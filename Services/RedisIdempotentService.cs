using RabbitMqDemo.Entities;
using RabbitMqDemo.Interfaces;
using StackExchange.Redis;
using System.Text.Json;

namespace RabbitMqDemo.Services
{
    /// <summary>
    /// Redis 消息幂等服务
    /// 基于 Redis SETNX + Lua 脚本实现原子幂等锁
    /// 每条消息对应一个 Redis key：mq:idempotent:{MsgId}
    /// key 的值：{"finished":true} 表示已处理完成
    /// TTL：队列 TTL + 30 秒，确保消息生命周期内幂等记录不提前过期
    /// </summary>
    public class RedisIdempotentService : IMsgIdempotentService
    {
        private readonly IDatabase _redisDb;
        /// <summary>
        /// Redis key 前缀，用于区分业务命名空间
        /// 完整 key 格式：mq:idempotent:msg_550e8400-e29b-41d4-a716-446655440000
        /// </summary>
        private const string KeyPrefix = "mq:idempotent:";

        /// <summary>
        /// Lua 脚本：原子检查 + 加锁
        /// 使用 cjson 解析 JSON 记录，全程在 Redis 服务端原子执行
        ///
        /// 返回：
        ///   1 = 可消费（首次 或 崩溃恢复后重新加锁）
        ///   0 = 跳过（已消费完成，Finished=true）
        ///
        /// 逻辑流程：
        ///   GET key → 存在？→ cjson.decode → Finished=true？→ return 0（跳过）
        ///                                           ↓ false
        ///                                     崩溃遗留，覆盖重锁
        ///                        → 不存在？→ SETEX 写 {finished:false}
        ///                        → return 1（可消费）
        /// </summary>
        private const string TryLockScript = @"
            local key = KEYS[1]               -- Redis key（完整路径）
            local json = ARGV[1]               -- JSON 值
            local ttl = tonumber(ARGV[2])      -- 过期时间（秒）
            local val = redis.call('GET', key)
            if val then
                local ok, record = pcall(cjson.decode, val)
                -- 注意：record['finished'] 是小写，对应 camelCase 序列化
                if ok and type(record) == 'table' and record['finished'] then
                    return 0  -- 已消费完成，跳过
                end
                -- finished=false 或解析失败 → 进程崩溃遗留锁，覆盖重锁
            end
            redis.call('SETEX', key, ttl, json)  -- SET + EXPIRE 原子操作
            return 1  -- 首次加锁 或 崩溃恢复";

        /// <summary>
        /// JSON 序列化配置：camelCase（Finished → finished）
        /// Lua 脚本中必须用小写名称访问
        /// </summary>
        private static readonly JsonSerializerOptions _jsonOpts = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public RedisIdempotentService(IConnectionMultiplexer multiplexer)
        {
            _redisDb = multiplexer.GetDatabase();
        }

        /// <summary>
        /// 尝试加锁（幂等检查）
        /// 通过 Lua 脚本原子完成：检查 → 判断 → 加锁
        /// </summary>
        /// <param name="msgId">消息唯一 ID</param>
        /// <param name="expireSecond">锁过期时间（秒），建议 = 队列 TTL + 30</param>
        /// <returns>true=首次消息可消费, false=已消费完成跳过</returns>
        public async Task<bool> TryLockMsgAsync(string msgId, int expireSecond)
        {
            var key = $"{KeyPrefix}{msgId}";
            var record = new IdempotentRecord { Finished = false };
            string json = JsonSerializer.Serialize(record, _jsonOpts);

            // 执行 Lua 脚本，返回 Redis 整数
            var result = await _redisDb.ScriptEvaluateAsync(
                TryLockScript,
                new RedisKey[] { key },
                new RedisValue[] { json, expireSecond });

            return (int)result == 1;
        }

        /// <summary>
        /// 标记消息消费完成
        /// 将 Finished 从 false 更新为 true
        /// When.Exists 确保 key 仍在有效期内才更新（避免过期后错误重建）
        /// </summary>
        public async Task MarkFinishAsync(string msgId)
        {
            var key = $"{KeyPrefix}{msgId}";
            var record = new IdempotentRecord { Finished = true };
            string json = JsonSerializer.Serialize(record, _jsonOpts);
            await _redisDb.StringSetAsync(key, json, when: When.Exists);
        }

        /// <summary>
        /// 释放幂等锁（业务失败时调用）
        /// 删除 Redis key，允许消息被重新消费
        /// </summary>
        public async Task UnLockMsgAsync(string msgId)
        {
            var key = $"{KeyPrefix}{msgId}";
            await _redisDb.KeyDeleteAsync(key);
        }
    }
}
