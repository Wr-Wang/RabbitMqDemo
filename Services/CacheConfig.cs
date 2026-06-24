using System;

namespace RabbitMqDemo.Services
{
    /// <summary>
    /// 缓存配置
    /// 从 appsettings.json 的 CacheConfig 节读取
    /// </summary>
    public class CacheConfig
    {
        /// <summary>默认过期时间（秒），默认 300s（5 分钟）</summary>
        public int DefaultExpirySeconds { get; set; } = 300;

        /// <summary>空值缓存过期时间（秒），默认 30s，穿透防护用</summary>
        public int NullCacheExpirySeconds { get; set; } = 30;

        /// <summary>锁超时（秒），默认 5s，击穿防护用</summary>
        public int LockTimeoutSeconds { get; set; } = 5;

        /// <summary>锁自旋最大重试次数，默认 10 次</summary>
        public int MaxLockRetries { get; set; } = 10;

        /// <summary>锁自旋重试间隔（毫秒），默认 100ms</summary>
        public int LockRetryDelayMs { get; set; } = 100;

        /// <summary>兜底写入 TTL（秒），自旋超时后直接写入缓存时使用</summary>
        public int FallbackExpirySeconds { get; set; } = 60;

        /// <summary>
        /// 🛡️ 雪崩防护：TTL 随机偏移最小值（比例）
        /// 默认 0.75 = 最短降至原 TTL 的 75%
        /// </summary>
        public double TtlJitterMin { get; set; } = 0.75;

        /// <summary>
        /// 🛡️ 雪崩防护：TTL 随机偏移最大值（比例）
        /// 默认 1.25 = 最长增至原 TTL 的 125%
        /// </summary>
        public double TtlJitterMax { get; set; } = 1.25;

        /// <summary>Bloom Filter 预计容量，默认 10000</summary>
        public int BloomFilterSize { get; set; } = 10000;

        /// <summary>Bloom Filter 哈希函数数量，默认 7</summary>
        public int BloomFilterHashCount { get; set; } = 7;
    }
}
