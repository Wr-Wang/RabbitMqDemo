using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMqDemo.Interfaces;
using RabbitMqDemo.Services;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqDemo.Extensions
{
    public static class RedisServiceCollectionExt
    {
        public static IServiceCollection AddRedisIdempotent(this IServiceCollection services, IConfiguration cfg)
        {
            string connStr = cfg["RedisConfig:ConnStr"];
            if (string.IsNullOrWhiteSpace(connStr))
            {
                throw new InvalidOperationException("RedisConfig:ConnStr 配置为空，请检查appsettings.json是否复制到输出目录");
            }

            var multiplexer = ConnectionMultiplexer.Connect(connStr);
            // 核心修复：注册接口 IConnectionMultiplexer
            services.AddSingleton<IConnectionMultiplexer>(multiplexer);
            services.AddSingleton<IMsgIdempotentService, RedisIdempotentService>();
            return services;
        }
    }
}
