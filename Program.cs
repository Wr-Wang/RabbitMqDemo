using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMqDemo.Entities;
using RabbitMqDemo.Extensions;
using RabbitMqDemo.Interfaces;
using StackExchange.Redis;

// ===== 控制台输出配置（必须放在最前面） =====
// 设置 UTF-8 编码确保中文日志正确输出
Console.OutputEncoding = System.Text.Encoding.UTF8;
// 重定向时启用 AutoFlush，避免输出被缓冲区卡住
if (Console.Out is System.IO.StreamWriter sw)
    sw.AutoFlush = true;

// ===== 构建依赖注入容器 =====
var host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration(cfgBuilder =>
    {
        // 加载 appsettings.json（必须存在，启动时重载）
        cfgBuilder.AddJsonFile("appsettings.json", reloadOnChange: true, optional: false);
    })
    .ConfigureServices((ctx, services) =>
    {
        // 注册顺序：先 Redis（提供 IConnectionMultiplexer），再 RabbitMQ
        services.AddRedisIdempotent(ctx.Configuration);
        services.AddRabbitMqHelper();
    })
    .Build();

// 获取应用程序生命周期（用于优雅关闭）
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

// 从 DI 容器解析 MQ 助手
using var scope = host.Services.CreateScope();
var mqHelper = scope.ServiceProvider.GetRequiredService<IRabbitMqHelper>();

try
{
    // ===== 第一步：初始化队列和交换机 =====
    mqHelper.InitDelayDlxQueue();

    // ===== 第二步：发送测试消息 =====
    Console.WriteLine("\n========== 第一阶段：发送15条消息（10条唯一 + 5条重复） ==========");

    // 发送 10 条唯一消息（模拟正常业务）
    for (int i = 1; i <= 10; i++)
    {
        var msg = new MqMessage
        {
            MsgId = $"msg_durable_test_{i:D3}",
            Data = $"业务数据_{i}"
        };
        await mqHelper.PublishConfirmAsync(msg);
        await Task.Delay(100); // 模拟业务间隔
    }
    Console.WriteLine(">> 10 条唯一消息发送完成");

    // 发送 5 条重复消息（MsgId 与前十的前五条相同）
    // 预期行为：消费者读取 Redis 发现 Finished=true → 跳过处理
    for (int i = 1; i <= 5; i++)
    {
        var msg = new MqMessage
        {
            MsgId = $"msg_durable_test_{i:D3}",
            Data = $"重复数据_{i}（应该被拦截）"
        };
        await mqHelper.PublishConfirmAsync(msg);
        await Task.Delay(100);
    }
    Console.WriteLine(">> 5 条重复消息发送完成（应全被幂等拦截）\n");

    // ===== 第三步：启动消费者 =====
    // 30 秒后自动退出（模拟 Ctrl+C，也可手动 Ctrl+C 提前退出）
    using var stopCts = CancellationTokenSource.CreateLinkedTokenSource(lifetime.ApplicationStopping);
    stopCts.CancelAfter(TimeSpan.FromSeconds(30));

    await mqHelper.StartConsumeAsync(msg =>
    {
        Console.WriteLine($"[业务处理] {msg.MsgId} | Data={msg.Data}");

        // 随机模拟业务失败：1/3 概率触发 Nack 重试
        // 重试机制：Nack requeue=true → 重新入队 → 再次投递 → 再次尝试
        //         超时进入死信（队列 x-message-ttl=8000ms）
        var rand = Random.Shared.Next(1, 4);
        if (rand == 2)
        {
            Console.WriteLine($"[业务失败] {msg.MsgId} 模拟异常，将 Nack 重试");
            return Task.FromResult(false);
        }

        Thread.Sleep(50); // 模拟业务耗时
        return Task.FromResult(true);
    }, stopCts.Token);
}
catch (OperationCanceledException)
{
    // 消费者通过 CancellationToken 正常退出
}
catch (Exception ex)
{
    Console.WriteLine($"全局异常：{ex.Message}");
}
finally
{
    // ===== 第四步：清理资源 =====
    mqHelper.Dispose();
    await host.StopAsync();
}

// ===== 第五步：验证 Redis 持久化数据 =====
Console.WriteLine("\n========== 第二阶段：验证 Redis 幂等记录 ==========");

try
{
    // 从 DI 容器获取 Redis 连接配置
    var cfg = host.Services.GetRequiredService<IConfiguration>();
    string connStr = cfg["RedisConfig:ConnStr"] ?? "127.0.0.1:6379,password=123456,defaultDatabase=0";
    var redis = ConnectionMultiplexer.Connect(connStr);
    var db = redis.GetDatabase();

    // 扫描所有以 mq:idempotent: 开头的 key
    var keys = new List<RedisKey>();
    var server = redis.GetServer(redis.GetEndPoints()[0]);
    foreach (var key in server.Keys(pattern: "mq:idempotent:*"))
    {
        keys.Add(key);
    }

    Console.WriteLine($"\nRedis 中幂等记录总数：{keys.Count}");

    if (keys.Count > 0)
    {
        // 输出每条幂等记录的 key 和值
        Console.WriteLine("\n--- 幂等记录明细 ---");
        foreach (var key in keys.OrderBy(k => k.ToString()))
        {
            var val = await db.StringGetAsync(key);
            Console.WriteLine($"  {key} => {val}");
        }

        // 抽样验证：检查第一条消息是否已标记完成
        var sampleKey = (RedisKey)"mq:idempotent:msg_durable_test_001";
        var sampleVal = await db.StringGetAsync(sampleKey);
        if (sampleVal.HasValue && sampleVal.ToString().Contains("true"))
        {
            Console.WriteLine("\n✅ 验证通过：msg_durable_test_001 已标记 Finished=true");
        }
        else
        {
            Console.WriteLine("\n⚠️ msg_durable_test_001 记录不存在或未完成");
        }
    }
    else
    {
        Console.WriteLine("\n⚠️ Redis 中没有找到幂等记录（可能已过期）");
    }

    redis.Dispose();
}
catch (Exception ex)
{
    Console.WriteLine($"\nRedis 验证异常：{ex.GetType().Name}: {ex.Message}");
}

Console.WriteLine("\n========== 演示结束 ==========");
