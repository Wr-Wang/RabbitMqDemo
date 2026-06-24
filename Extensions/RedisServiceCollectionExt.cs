using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMqDemo.Interfaces;
using RabbitMqDemo.Services;
using StackExchange.Redis;

namespace RabbitMqDemo.Extensions
{
    /// <summary>
    /// Redis 服务注册扩展方法
    /// 将 IConnectionMultiplexer 和 IMsgIdempotentService 注册到 DI 容器
    /// </summary>
    public static class RedisServiceCollectionExt
    {
        /// <summary>
        /// 注册 Redis 幂等服务
        /// 使用延迟创建 + 容错配置，应用启动时不因 Redis 不可用而崩溃
        /// </summary>
        public static IServiceCollection AddRedisIdempotent(this IServiceCollection services, IConfiguration cfg)
        {
            string connStr = cfg["RedisConfig:ConnStr"]
                ?? throw new InvalidOperationException("RedisConfig:ConnStr 配置为空");

            // 延迟创建 IConnectionMultiplexer：
            // - 使用工厂委托，首次 GetRequiredService 时才创建（懒加载）
            // - AbortOnConnectFail=false：连接失败不抛异常，后台自动重试
            // - ConnectTimeout=3000：3 秒超时，避免长时间阻塞启动
            services.AddSingleton<IConnectionMultiplexer>(_ =>
            {
                var opts = ConfigurationOptions.Parse(connStr);
                opts.AbortOnConnectFail = false;
                opts.ConnectTimeout = 3000;
                return ConnectionMultiplexer.Connect(opts);
            });

            // 注册幂等服务（单例，共享同一个 Redis 连接）
            services.AddSingleton<IMsgIdempotentService, RedisIdempotentService>();
            return services;
        }
    }
}
