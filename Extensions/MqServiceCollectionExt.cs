using Microsoft.Extensions.DependencyInjection;
using RabbitMqDemo.Interfaces;
using RabbitMqDemo.Services;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqDemo.Extensions
{
    public static class MqServiceCollectionExt
    {
        public static IServiceCollection AddRabbitMqHelper(this IServiceCollection services)
        {
            services.AddSingleton<IRabbitMqHelper, RabbitMqHelper>();
            return services;
        }
    }
}
