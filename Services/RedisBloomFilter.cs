using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMqDemo.Interfaces;
using StackExchange.Redis;

namespace RabbitMqDemo.Services
{
    /// <summary>
    /// Redis 布隆过滤器（Bloom Filter）
    ///
    /// 使用 Redis 的 SETBIT / GETBIT 命令实现位数组操作，
    /// 采用 Kirsch-Mitzenmacher 优化：仅需两个基础哈希即可生成 k 个哈希位置。
    /// hash_i = (h1 + i * h2) % bloomSize
    ///
    /// 用于缓存穿透防护——快速判断某个 key 是否一定不存在于后端存储。
    /// Bloom Filter 判定「不存在」= 100% 不存在；判定「存在」= 可能存在（有误判率）。
    /// </summary>
    public class RedisBloomFilter : IRedisBloomFilter
    {
        private readonly IDatabase _db;
        private readonly ILogger<RedisBloomFilter> _logger;
        private readonly CacheConfig _config;

        // FNV-1a 哈希算法的两个不同种子
        private const ulong FNV1A_SEED_1 = 14695981039346656037;
        private const ulong FNV1A_SEED_2 = 1099511628211;

        public RedisBloomFilter(
            IConnectionMultiplexer redis,
            ILogger<RedisBloomFilter> logger,
            CacheConfig config)
        {
            _db = redis.GetDatabase();
            _logger = logger;
            _config = config;
        }

        /// <summary>
        /// 添加单个元素到 Bloom Filter
        /// </summary>
        public async Task AddAsync(string key, string element)
        {
            var (h1, h2) = ComputeHashes(element);
            var bloomSize = _config.BloomFilterSize;

            for (int i = 0; i < _config.BloomFilterHashCount; i++)
            {
                var bitIndex = (int)((h1 + (ulong)i * h2) % (ulong)bloomSize);
                await _db.StringSetBitAsync(key, bitIndex, true);
            }
        }

        /// <summary>
        /// 批量添加元素到 Bloom Filter
        /// </summary>
        public async Task AddAsync(string key, params string[] elements)
        {
            foreach (var element in elements)
            {
                await AddAsync(key, element);
            }

            _logger.LogDebug("Bloom Filter 批量添加 {Count} 个元素到 {Key}", elements.Length, key);
        }

        /// <summary>
        /// 判断元素是否可能存在
        /// false = 一定不存在（穿透拦截）
        /// true  = 可能存在（有误判可能，需继续查询）
        /// </summary>
        public async Task<bool> ContainsAsync(string key, string element)
        {
            var (h1, h2) = ComputeHashes(element);
            var bloomSize = _config.BloomFilterSize;

            for (int i = 0; i < _config.BloomFilterHashCount; i++)
            {
                var bitIndex = (int)((h1 + (ulong)i * h2) % (ulong)bloomSize);
                var bit = await _db.StringGetBitAsync(key, bitIndex);
                if (!bit) return false; // 任意一位为 0 ⇒ 一定不存在
            }

            return true; // 所有位都为 1 ⇒ 可能存在
        }

        /// <summary>
        /// 计算两个基础哈希值（FNV-1a + Kirsch-Mitzenmacher 优化）
        /// h1 和 h2 用于线性组合生成 k 个不同的哈希位置
        /// </summary>
        private static (ulong h1, ulong h2) ComputeHashes(string element)
        {
            var bytes = Encoding.UTF8.GetBytes(element);
            ulong h1 = Fnv1A(bytes, FNV1A_SEED_1);
            ulong h2 = Fnv1A(bytes, FNV1A_SEED_2);
            return (h1, h2);
        }

        /// <summary>
        /// FNV-1a 哈希算法
        /// 简单快速，分布均匀，适合 Bloom Filter 使用
        /// </summary>
        private static ulong Fnv1A(byte[] data, ulong seed)
        {
            ulong hash = seed;
            foreach (var b in data)
            {
                hash ^= b;
                hash *= 1099511628211;
            }
            return hash;
        }
    }
}
