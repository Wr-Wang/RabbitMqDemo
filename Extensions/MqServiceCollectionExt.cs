using Microsoft.Extensions.DependencyInjection;
using RabbitMqDemo.Interfaces;
using RabbitMqDemo.Services;

namespace RabbitMqDemo.Extensions
{
    /// <summary>
    /// RabbitMQ 服务注册扩展方法
    /// </summary>
    public static class MqServiceCollectionExt
    {
        /// <summary>
        /// 注册 RabbitMQ 助手为单例
        /// 单例作用域内共享同一个连接和通道
        /// </summary>
        public static IServiceCollection AddRabbitMqHelper(this IServiceCollection services)
        {
            services.AddSingleton<IRabbitMqHelper, RabbitMqHelper>();
            return services;
        }
    }
}
