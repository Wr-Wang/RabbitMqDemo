using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMqDemo.Interfaces;
using StackExchange.Redis;

namespace RabbitMqDemo.Services
{
    /// <summary>
    /// Redis 缓存服务 —— 内置缓存雪崩 / 击穿 / 穿透三大防护策略
    ///
    /// ██ 防护总览 ██
    /// ┌─────────────────────────────────────────────────────────┐
    /// │  ① 穿透防护（Cache Penetration）                         │
    /// │     策略 A：空值缓存（Null Cache）                        │
    /// │     策略 B：布隆过滤器（Bloom Filter）预检                │
    /// ├─────────────────────────────────────────────────────────┤
    /// │  ② 击穿防护（Cache Breakdown / Hotkey Invalid）          │
    /// │     策略：分布式互斥锁（SETNX），单线程回源加载            │
    /// ├─────────────────────────────────────────────────────────┤
    /// │  ③ 雪崩防护（Cache Avalanche）                           │
    /// │     策略：TTL 添加随机偏移（±25%），避免批量同时过期      │
    /// └─────────────────────────────────────────────────────────┘
    /// </summary>
    public class RedisCacheService : ICacheService
    {
        private readonly IDatabase _db;
        private readonly IRedisBloomFilter _bloomFilter;
        private readonly ILogger<RedisCacheService> _logger;
        private readonly CacheConfig _config;

        // ===== 空值标记（缓存穿透防护用） =====
        // 当数据库中不存在对应记录时，缓存此标记而非真实数据
        // 以短 TTL 缓存，防止恶意穿透请求反复穿透到 DB
        private static readonly RedisValue NULL_SENTINEL = (RedisValue)"__CACHE_NULL__";

        // ===== 缓存键名前缀 =====
        private const string KEY_PREFIX = "cache:data:";
        private const string LOCK_PREFIX = "cache:lock:";
        private const string BLOOM_NAMESPACE = "cache:bloom";

        // ===== Lua 脚本：原子释放锁 =====
        // 仅当锁的持有者是自己时才删除，防止误删其他线程的锁
        private const string UNLOCK_SCRIPT = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
        ";

        private readonly JsonSerializerOptions _jsonOpts;

        public RedisCacheService(
            IConnectionMultiplexer redis,
            IRedisBloomFilter bloomFilter,
            ILogger<RedisCacheService> logger,
            CacheConfig config)
        {
            _db = redis.GetDatabase();
            _bloomFilter = bloomFilter;
            _logger = logger;
            _config = config;

            _jsonOpts = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        // =================================================================
        //  公开方法
        // =================================================================

        /// <summary>
        /// 获取缓存 —— 若未命中则通过 fallback 加载，自动应用三大防护策略
        /// </summary>
        public async Task<T?> GetOrSetAsync<T>(string key, Func<Task<T?>> dbFallback, TimeSpan? expiry = null)
        {
            // ============================================================
            //  第 1 步：尝试从缓存读取
            // ============================================================
            var raw = await _db.StringGetAsync(BuildDataKey(key));

            if (raw.HasValue)
            {
                // ---- 穿透防护：命中空值标记，直接返回默认值 ----
                if (raw == NULL_SENTINEL)
                {
                    _logger.LogDebug("[穿透防护] 空值缓存命中 key={Key}，拦截穿透请求", key);
                    return default;
                }

                // ---- 缓存命中：反序列化返回 ----
                try
                {
                    return JsonSerializer.Deserialize<T>((string)raw!, _jsonOpts);
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "缓存反序列化失败 key={Key}，将重新加载", key);
                    // 数据损坏，回退到重新加载
                }
            }

            // ============================================================
            //  第 2 步：穿透防护 —— Bloom Filter 预检（可选增强层）
            //  如果 Bloom Filter 判定 key 一定不存在，直接拦截 + 缓存空值
            // ============================================================
            if (await _bloomFilter.ContainsAsync(BLOOM_NAMESPACE, key) == false)
            {
                _logger.LogDebug("[穿透防护] Bloom Filter 拦截 key={Key}，判定一定不存在", key);
                // 缓存空值（短 TTL）防止后续相同穿透请求
                await CacheNullAsync(key, expiry);
                return default;
            }

            // ============================================================
            //  第 3 步：击穿防护 —— 分布式互斥锁
            //  防止缓存失效瞬间大量并发请求同时回源到 DB
            // ============================================================
            var lockKey = BuildLockKey(key);
            var lockValue = Guid.NewGuid().ToString("N"); // 唯一标识，用于安全释放锁
            var lockTtl = TimeSpan.FromSeconds(_config.LockTimeoutSeconds);

            if (await _db.StringSetAsync(lockKey, lockValue, lockTtl, When.NotExists))
            {
                // ─── 成功获取锁 ───
                try
                {
                    // 双检：获取锁后再次检查缓存（可能已被上一个锁持有者写入）
                    raw = await _db.StringGetAsync(BuildDataKey(key));
                    if (raw.HasValue)
                    {
                        if (raw == NULL_SENTINEL) return default;
                        return JsonSerializer.Deserialize<T>((string)raw!, _jsonOpts);
                    }

                    // ─── 回源 DB 加载数据 ───
                    _logger.LogDebug("[击穿防护] 获取锁成功，回源加载 key={Key}", key);
                    var result = await dbFallback();

                    if (result is null)
                    {
                        // ---- 穿透防护：数据不存在，缓存空值 ----
                        await CacheNullAsync(key, expiry);
                        return default;
                    }

                    // ---- 雪崩防护：写入缓存时对 TTL 添加随机偏移 ----
                    var finalExpiry = ApplyAvalancheJitter(expiry ?? TimeSpan.FromSeconds(_config.DefaultExpirySeconds));
                    await _db.StringSetAsync(BuildDataKey(key), JsonSerializer.Serialize(result, _jsonOpts), finalExpiry);

                    // ---- 写入 Bloom Filter ----
                    await _bloomFilter.AddAsync(BLOOM_NAMESPACE, key);

                    _logger.LogDebug("[雪崩防护] key={Key} TTL={TotalSeconds:F1}s（含随机偏移）", key, finalExpiry.TotalSeconds);
                    return result;
                }
                finally
                {
                    // 原子释放锁（仅当仍为自己持有）
                    await _db.ScriptEvaluateAsync(UNLOCK_SCRIPT, [lockKey], [lockValue]);
                }
            }
            else
            {
                // ─── 未获取到锁：自旋等待，重试读取缓存 ───
                _logger.LogDebug("[击穿防护] 锁被占用，进入自旋等待 key={Key}", key);

                for (int i = 0; i < _config.MaxLockRetries; i++)
                {
                    await Task.Delay(_config.LockRetryDelayMs);

                    raw = await _db.StringGetAsync(BuildDataKey(key));
                    if (raw.HasValue)
                    {
                        if (raw == NULL_SENTINEL) return default;
                        return JsonSerializer.Deserialize<T>((string)raw!, _jsonOpts);
                    }
                }

                // ─── 自旋超时：最后兜底策略，直查 DB（无锁保护） ───
                _logger.LogWarning("[击穿防护] 自旋超时（{Retries}次），直查 DB 兜底 key={Key}", _config.MaxLockRetries, key);
                var fallbackResult = await dbFallback();

                if (fallbackResult is not null)
                {
                    // 兜底写入，用较短的 TTL
                    var fallbackExpiry = TimeSpan.FromSeconds(_config.FallbackExpirySeconds);
                    await _db.StringSetAsync(BuildDataKey(key), JsonSerializer.Serialize(fallbackResult, _jsonOpts), fallbackExpiry);
                }

                return fallbackResult;
            }
        }

        /// <summary>
        /// 设置缓存（自动添加随机 TTL 偏移，防止雪崩）
        /// </summary>
        public async Task SetAsync<T>(string key, T value, TimeSpan? expiry = null)
        {
            var finalExpiry = ApplyAvalancheJitter(expiry ?? TimeSpan.FromSeconds(_config.DefaultExpirySeconds));
            await _db.StringSetAsync(BuildDataKey(key), JsonSerializer.Serialize(value, _jsonOpts), finalExpiry);
            await _bloomFilter.AddAsync(BLOOM_NAMESPACE, key);

            _logger.LogDebug("[雪崩防护] Set key={Key} TTL={TotalSeconds:F1}s（含随机偏移）", key, finalExpiry.TotalSeconds);
        }

        /// <summary>
        /// 删除缓存
        /// </summary>
        public async Task RemoveAsync(string key)
        {
            await _db.KeyDeleteAsync(BuildDataKey(key));
            _logger.LogDebug("删除缓存 key={Key}", key);
        }

        /// <summary>
        /// 仅获取缓存（不触发任何防护策略和 fallback）
        /// </summary>
        public async Task<T?> GetAsync<T>(string key)
        {
            var raw = await _db.StringGetAsync(BuildDataKey(key));
            if (!raw.HasValue || raw == NULL_SENTINEL)
                return default;

            return JsonSerializer.Deserialize<T>((string)raw!, _jsonOpts);
        }

        // =================================================================
        //  内部方法
        // =================================================================

        /// <summary>
        /// 🛡️ 雪崩防护：对 TTL 添加随机偏移（Jitter）
        /// 默认 ±25% 范围内随机抖动，使缓存过期时间分散
        /// 避免大量 key 在同一时刻过期导致 DB 负载尖峰
        /// </summary>
        private TimeSpan ApplyAvalancheJitter(TimeSpan baseExpiry)
        {
            var jitter = Random.Shared.NextDouble() * (_config.TtlJitterMax - _config.TtlJitterMin) + _config.TtlJitterMin;
            return TimeSpan.FromMilliseconds(baseExpiry.TotalMilliseconds * jitter);
        }

        /// <summary>
        /// 🛡️ 穿透防护：缓存空值标记
        /// 对不存在的数据缓存一个短 TTL 的空标记，后续相同 key 的请求
        /// 直接返回空而不再穿透到 DB
        /// </summary>
        private async Task CacheNullAsync(string key, TimeSpan? originalExpiry)
        {
            // 空值缓存使用更短的 TTL：取 NullCacheExpirySeconds 和原 TTL 的较小值
            var nullTtlSeconds = Math.Min(
                _config.NullCacheExpirySeconds,
                originalExpiry?.TotalSeconds ?? _config.NullCacheExpirySeconds);

            await _db.StringSetAsync(BuildDataKey(key), NULL_SENTINEL, TimeSpan.FromSeconds(nullTtlSeconds));

            _logger.LogDebug("[穿透防护] 空值缓存写入 key={Key} TTL={Seconds}s", key, nullTtlSeconds);
        }

        private static string BuildDataKey(string key) => $"{KEY_PREFIX}{key}";
        private static string BuildLockKey(string key) => $"{LOCK_PREFIX}{key}";
    }
}
