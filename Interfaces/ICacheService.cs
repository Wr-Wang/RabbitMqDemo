using System;
using System.Threading.Tasks;

namespace RabbitMqDemo.Interfaces
{
    /// <summary>
    /// 通用缓存服务接口
    /// 内置缓存雪崩、击穿、穿透三大防护策略
    /// </summary>
    public interface ICacheService
    {
        /// <summary>
        /// 获取缓存，若不存在则通过 dbFallback 加载并回填
        /// 自动应用三大防护策略：
        ///   🛡️ 穿透防护：空值缓存 + Bloom Filter
        ///   🛡️ 击穿防护：分布式互斥锁
        ///   🛡️ 雪崩防护：随机 TTL 偏移
        /// </summary>
        Task<T?> GetOrSetAsync<T>(string key, Func<Task<T?>> dbFallback, TimeSpan? expiry = null);

        /// <summary>
        /// 直接设置缓存（自动添加随机 TTL 偏移，防止雪崩）
        /// </summary>
        Task SetAsync<T>(string key, T value, TimeSpan? expiry = null);

        /// <summary>
        /// 删除缓存
        /// </summary>
        Task RemoveAsync(string key);

        /// <summary>
        /// 仅获取缓存（不触发 fallback，不做任何防护处理）
        /// </summary>
        Task<T?> GetAsync<T>(string key);
    }
}
