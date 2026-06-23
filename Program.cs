//Console.WriteLine("Hello, World!");
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMqDemo.Entities;
using RabbitMqDemo.Extensions;
using RabbitMqDemo.Interfaces;

// 构建DI主机
var host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration(cfgBuilder =>
    {
        cfgBuilder.AddJsonFile("appsettings.json", reloadOnChange: true, optional: false);
    })
    .ConfigureServices((ctx, services) =>
    {
        // 注册顺序：先Redis（提供IConnectionMultiplexer），再RabbitMQ
        services.AddRedisIdempotent(ctx.Configuration);
        services.AddRabbitMqHelper();
    })
    .Build();

// 获取MQ工具实例
using var scope = host.Services.CreateScope();
var mqHelper = scope.ServiceProvider.GetRequiredService<IRabbitMqHelper>();

try
{
    // 初始化TTL+死信队列绑定
    mqHelper.InitDelayDlxQueue();

    Console.WriteLine("\n===== 批量发送测试消息 =====");
    // 批量发送6条测试消息
    for (int i = 1; i <= 6; i++)
    {
        var msg = new MqMessage
        {
            MsgId = $"msg_{Guid.NewGuid():N}",
            Data = $"业务测试数据_{i}"
        };
        await mqHelper.PublishConfirmAsync(msg);
        await Task.Delay(200);
    }
    Console.WriteLine("===== 全部消息发送完成 =====\n");

    // 启动消费者，传入业务处理委托
    await mqHelper.StartConsumeAsync(async msg =>
    {
        Console.WriteLine($"[业务处理中] {msg.MsgId}");
        // 随机模拟失败：1/3概率返回false触发Nack重试消费者启动完成
        var rand = Random.Shared.Next(1, 4);
        if (rand == 2)
        {
            Console.WriteLine($"[模拟业务异常] {msg.MsgId} 处理失败");
            return false;
        }
        await Task.Delay(150);
        return true;
    });
}
catch (Exception ex)
{
    Console.WriteLine($"全局异常：{ex.Message}");
}
finally
{
    mqHelper.Dispose();
    await host.StopAsync();
}