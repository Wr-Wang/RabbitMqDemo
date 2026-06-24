using RabbitMqDemo.Entities;
using RabbitMqDemo.Interfaces;
using StackExchange.Redis;
using System.Text.Json;

namespace RabbitMqDemo.Services
{
    /// <summary>
    /// Redis 消息记录服务
    ///
    /// 存储方式：每条消息以 JSON 字符串存入 Redis String
    ///   key = mq:record:{MsgId}
    ///   val = {"msgId":"...","data":"...","status":"未消费","createTs":...,"consumeTs":null,"dlqTs":null,"retryCount":0}
    ///
    /// 与 IMsgIdempotentService 的关系：
    ///   - 幂等服务 (mq:idempotent:*)：用于运行时原子加锁/幂等判断，短 TTL
    ///   - 记录服务 (mq:record:*)    ：用于全生命周期追踪，无 TTL（持久保存）
    /// </summary>
    public class RedisMsgRecordService : IMsgRecordService
    {
        private readonly IDatabase _redisDb;
        private const string RecordKeyPrefix = "mq:record:";

        private static readonly JsonSerializerOptions _jsonOpts = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public RedisMsgRecordService(IConnectionMultiplexer multiplexer)
        {
            _redisDb = multiplexer.GetDatabase();
        }

        /// <summary>
        /// 创建消息初始记录
        /// 若 key 已存在则跳过（防止重复发布覆盖已消费状态）
        /// </summary>
        public async Task CreateMessageRecordAsync(MqMessage msg)
        {
            var key = $"{RecordKeyPrefix}{msg.MsgId}";
            bool exists = await _redisDb.KeyExistsAsync(key);
            if (exists)
            {
                // key 已存在，说明该 MsgId 之前已发布过（重复消息场景）
                // 不覆盖已有记录，保留上一次的消费状态
                return;
            }

            var record = new MessageRecord
            {
                MsgId = msg.MsgId,
                Data = msg.Data,
                Status = MessageStatuses.Published,
                CreateTs = msg.CreateTs,
                RetryCount = 0
            };
            string json = JsonSerializer.Serialize(record, _jsonOpts);
            await _redisDb.StringSetAsync(key, json);
        }

        /// <summary>
        /// 更新消息记录的状态
        /// 自动记录关键时间戳：
        ///   - 首次标记 已消费 → 记录 ConsumeTs
        ///   - 首次标记 已进死信 → 记录 DlqTs
        /// </summary>
        public async Task UpdateStatusAsync(string msgId, string status, int? retryCount = null)
        {
            var key = $"{RecordKeyPrefix}{msgId}";
            var json = await _redisDb.StringGetAsync(key);
            if (!json.HasValue) return; // 记录不存在，忽略

            var record = JsonSerializer.Deserialize<MessageRecord>((string)json!, _jsonOpts);
            if (record == null) return;

            record.Status = status;
            if (retryCount.HasValue)
                record.RetryCount = retryCount.Value;

            string nowStr = DateTime.Now.ToString("yyyy.MM.dd HH:mm:ss.fff");

            // 首次消费成功 → 记录消费时间
            if (status == MessageStatuses.Consumed && record.ConsumeTs == null)
                record.ConsumeTs = nowStr;

            // 首次进入死信 → 记录死信时间
            if (status == MessageStatuses.DeadLettered && record.DlqTs == null)
                record.DlqTs = nowStr;

            string updatedJson = JsonSerializer.Serialize(record, _jsonOpts);
            await _redisDb.StringSetAsync(key, updatedJson);
        }

        /// <summary>
        /// 获取单条消息记录
        /// </summary>
        public async Task<MessageRecord?> GetMessageRecordAsync(string msgId)
        {
            var json = await _redisDb.StringGetAsync($"{RecordKeyPrefix}{msgId}");
            if (!json.HasValue) return null;
            return JsonSerializer.Deserialize<MessageRecord>((string)json!, _jsonOpts);
        }

        /// <summary>
        /// 获取所有消息记录（通过 SCAN 扫描 mq:record:*）
        /// </summary>
        public async Task<List<MessageRecord>> GetAllMessageRecordsAsync()
        {
            var records = new List<MessageRecord>();
            var server = _redisDb.Multiplexer.GetServer(_redisDb.Multiplexer.GetEndPoints()[0]);
            foreach (var key in server.Keys(pattern: $"{RecordKeyPrefix}*"))
            {
                var json = await _redisDb.StringGetAsync(key);
                if (json.HasValue)
                {
                    var record = JsonSerializer.Deserialize<MessageRecord>((string)json!, _jsonOpts);
                    if (record != null)
                        records.Add(record);
                }
            }
            return records;
        }
    }
}
