using System.Threading.Tasks;

namespace RabbitMqDemo.Interfaces
{
    /// <summary>
    /// Redis 布隆过滤器接口
    /// 用于缓存穿透防护：快速判断 key 是否一定不存在
    /// 如果 Bloom Filter 判定不存在，则数据一定不存在，可直接拦截
    /// </summary>
    public interface IRedisBloomFilter
    {
        /// <summary>
        /// 添加元素到 Bloom Filter
        /// </summary>
        Task AddAsync(string key, string element);

        /// <summary>
        /// 批量添加元素
        /// </summary>
        Task AddAsync(string key, params string[] elements);

        /// <summary>
        /// 判断元素是否可能存在
        /// false = 一定不存在（穿透拦截）｜ true = 可能存在（需继续查询）
        /// </summary>
        Task<bool> ContainsAsync(string key, string element);
    }
}
