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

// 从 DI 容器解析服务
using var scope = host.Services.CreateScope();
var mqHelper = scope.ServiceProvider.GetRequiredService<IRabbitMqHelper>();
var msgRecordSvc = scope.ServiceProvider.GetRequiredService<IMsgRecordService>();

// 死信消息计数器 + 后台任务句柄（在 try/finally/Block7 中共享，必须在 try 前声明）
int dlqMsgCount = 0;
Task? dlqTask = null;
// 本次运行的消息 ID 集合（用于 Block 2b 构造重复消息 + Block 6 验证）
string? firstMsgId = null;
List<string> runMsgIds = [];

try
{
    // ================================================================
    // Block 1: 初始化队列和死信交换机
    // ================================================================
    Console.WriteLine("\n========== Block1: 初始化队列和死信交换机 ==========");
    Console.WriteLine("  普通队列: biz.normal.queue -> 绑定死信交换机 biz.dlx.ex (TTL=8000ms)");
    Console.WriteLine("  死信队列: biz.dlx.queue   <- 消费失败超时的消息");

    mqHelper.InitDelayDlxQueue();

    Console.WriteLine("✅ 队列/交换机初始化完成\n");

    // ================================================================
    // Block 2: 发送测试消息
    //   2a: 10 条唯一消息（模拟正常业务）
    //   2b: 5  条重复消息（MsgId 与 2a 前五条相同，验证幂等拦截）
    //   2c: 快速验证消息持久化到 Redis
    // ================================================================
    Console.WriteLine("========== Block2: 发送测试消息 ==========");

    // ---- Block 2a: 10 条唯一消息（MsgId 基于运行时间戳，每次重启不同） ----
    string runTag = DateTime.Now.ToString("yyyyMMddHHmmss");
    Console.WriteLine("\n>>> 2a: 发送10条唯一消息（验证 Publisher Confirm）");
    Console.WriteLine($"      本次运行标签: {runTag}");
    Console.WriteLine("      MsgId 格式: msg_{runTag}_001 ~ msg_{runTag}_010\n");

    for (int i = 1; i <= 10; i++)
    {
        string msgId = $"msg_{runTag}_{i:D3}";
        if (i == 1) firstMsgId = msgId;
        runMsgIds.Add(msgId);

        var msg = new MqMessage
        {
            MsgId = msgId,
            Data = $"业务数据_{i}"
        };
        await mqHelper.PublishConfirmAsync(msg);
        await Task.Delay(100); // 模拟业务间隔
    }
    Console.WriteLine("✅ 10 条唯一消息发送完成（均收到 Broker 确认）\n");

    // ---- Block 2b: 5 条重复消息（复用 2a 的前 5 个 MsgId，验证幂等拦截） ----
    Console.WriteLine(">>> 2b: 发送5条重复消息（幂等拦截测试）");
    Console.WriteLine("      复用 2a 的前 5 个 MsgId，模拟重复投递");
    Console.WriteLine("      预期：消费者发现 Redis 中 Finished=true → 跳过处理\n");

    for (int i = 0; i < 5; i++)
    {
        var msg = new MqMessage
        {
            MsgId = runMsgIds[i],
            Data = $"重复数据_{i + 1}（应被幂等拦截）"
        };
        await mqHelper.PublishConfirmAsync(msg);
        await Task.Delay(100);
    }
    Console.WriteLine("✅ 5 条重复消息发送完成（等待消费者幂等拦截验证）\n");

    // ---- Block 2c: 快速验证消息已持久化到 Redis ----
    Console.WriteLine(">>> 2c: 验证消息持久化（检查 Redis 中消息记录）");

    var verifyTarget = firstMsgId ?? runMsgIds[0];
    var verifyRecord = await msgRecordSvc.GetMessageRecordAsync(verifyTarget);
    bool persistOk = verifyRecord != null;
    Console.WriteLine(persistOk
        ? $"  ✅ {verifyTarget} 已持久化到 Redis | 状态={verifyRecord!.Status} | 创建时间={verifyRecord.CreateTs}"
        : $"  ⚠️  {verifyTarget} 未在 Redis 中找到（可能 Redis 不可用）");

    Console.WriteLine($"\n✅ 消息持久化验证{(persistOk ? "通过" : "失败")}\n");

    // ================================================================
    // Block 3: 启动死信队列消费者（后台任务，先启动防止漏收死信）
    // ================================================================
    Console.WriteLine("========== Block3: 启动死信队列消费者 ==========");
    Console.WriteLine("  死信消费者在后台监听 biz.dlx.queue");
    Console.WriteLine("  业务失败 → Nack requeue → TTL超时 → DLQ → 死信消费\n");

    var dlqHandler = new Func<MqMessage, Task<bool>>(msg =>
    {
        Interlocked.Increment(ref dlqMsgCount);
        Console.WriteLine($"  ↳ [死信处理] MsgId={msg.MsgId} | Data={msg.Data}");
        return Task.FromResult(true);
    });

    // 30 秒后自动退出（模拟 Ctrl+C，也可手动 Ctrl+C 提前退出）
    using var stopCts = CancellationTokenSource.CreateLinkedTokenSource(lifetime.ApplicationStopping);
    stopCts.CancelAfter(TimeSpan.FromSeconds(30));

    // 后台启动死信消费者
    dlqTask = mqHelper.StartDlxConsumeAsync(dlqHandler, stopCts.Token);

    Console.WriteLine("✅ 死信队列消费者已启动\n");

    // ================================================================
    // Block 4: 启动普通队列消费者（含 Redis 幂等校验 + 失败重试入死信）
    // ================================================================
    Console.WriteLine("========== Block4: 启动普通队列消费者 ==========");
    Console.WriteLine("  特性:");
    Console.WriteLine("    ① Redis 幂等去重（SETNX + Lua 原子操作）");
    Console.WriteLine("    ② 手动 ACK（处理成功 → Ack，标记 Finished=true）");
    Console.WriteLine("    ③ 业务失败 → Nack requeue → 超时入死信");
    Console.WriteLine("    ④ 消息持久化 → Redis 记录全生命周期状态");
    Console.WriteLine("    ⑤ 异常兜底 → Nack 不重入（避免卡死消费者）\n");

    await mqHelper.StartConsumeAsync(msg =>
    {
        Console.WriteLine($"[业务处理] MsgId={msg.MsgId} | Data={msg.Data}");

        // 随机模拟业务失败：1/3 概率触发 Nack 重试
        // 重试机制：Nack requeue=true → 重新入队 → 再次投递 → 再次尝试
        //           超时进入死信（队列 x-message-ttl=8000ms）
        var rand = Random.Shared.Next(1, 4);
        if (rand == 2)
        {
            Console.WriteLine($"[业务失败] {msg.MsgId} 模拟异常，将 Nack 重试（超时后进入死信）");
            return Task.FromResult(false);
        }

        Thread.Sleep(50); // 模拟业务耗时
        return Task.FromResult(true);
    }, stopCts.Token);
}
catch (OperationCanceledException)
{
    // 消费者通过 CancellationToken 正常退出（30 秒超时）
}
catch (Exception ex)
{
    Console.WriteLine($"全局异常：{ex.GetType().Name}: {ex.Message}");
}
finally
{
    // ================================================================
    // Block 5: 停止消费者 & 清理资源
    // ================================================================
    Console.WriteLine("\n========== Block5: 停止消费者 & 清理资源 ==========");
    Console.WriteLine("  主消费者与死信消费者共享 CancellationToken");
    Console.WriteLine("  超时或 Ctrl+C 触发同时关闭\n");

    // 等待死信消费者退出（主消费者已在 StartConsumeAsync 中正常退出）
    // 若 try 块早期就异常退出，dlqTask 可能为 null
    if (dlqTask is not null)
    {
        try { await dlqTask; } catch (OperationCanceledException) { }
    }

    Console.WriteLine($"📊 死信队列共处理 {dlqMsgCount} 条死信消息");

    mqHelper.Dispose();
    await host.StopAsync();

    Console.WriteLine("✅ 资源清理完成\n");
}

// ================================================================
// Block 6: 验证消息全生命周期记录 + Redis 幂等记录
// ================================================================
Console.WriteLine("========== Block6: 验证消息全生命周期记录 ==========");

try
{
    // ---- Part A: 消息记录（mq:record:*）— 全生命周期追踪 ----
    Console.WriteLine("\n--- Part A: 消息记录（全生命周期追踪） ---");

    using var redis = ConnectionMultiplexer.Connect(
        (host.Services.GetRequiredService<IConfiguration>())["RedisConfig:ConnStr"]
        ?? "127.0.0.1:6379,password=123456,defaultDatabase=0");
    var db = redis.GetDatabase();
    var server = redis.GetServer(redis.GetEndPoints()[0]);

    var allRecords = new List<MessageRecord>();
    foreach (var key in server.Keys(pattern: "mq:record:*"))
    {
        var json = await db.StringGetAsync(key);
        if (json.HasValue)
        {
            var rec = System.Text.Json.JsonSerializer.Deserialize<MessageRecord>((string)json!,
                new System.Text.Json.JsonSerializerOptions { PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase });
            if (rec != null) allRecords.Add(rec);
        }
    }

    Console.WriteLine($"消息记录总数：{allRecords.Count}");

    if (allRecords.Count > 0)
    {
        Console.WriteLine("\n--- 全部消息状态明细 ---");
        foreach (var rec in allRecords.OrderBy(r => r.MsgId))
        {
            string tsInfo = $"创建={rec.CreateTs}";
            if (rec.ConsumeTs != null) tsInfo += $" | 消费={rec.ConsumeTs}";
            if (rec.DlqTs != null) tsInfo += $" | 死信={rec.DlqTs}";
            Console.WriteLine($"  {rec.MsgId,-25} | {rec.Status,-8} | retry={rec.RetryCount} | {tsInfo}");
        }

        // 状态分布统计
        var dist = allRecords.GroupBy(r => r.Status)
                             .ToDictionary(g => g.Key, g => g.Count());
        Console.WriteLine("\n--- 状态分布 ---");
        foreach (var status in new[] { "未消费", "已消费", "已进死信", "幂等跳过", "消费失败" })
        {
            int count = dist.GetValueOrDefault(status, 0);
            string icon = count > 0 ? "●" : "○";
            Console.WriteLine($"  {icon} {status}: {count} 条");
        }

        Console.WriteLine("\n✅ 消息记录完整：每条消息从发布到消费/死信的完整链路已记录");
    }
    else
    {
        Console.WriteLine("\n⚠️  没有找到消息记录（Redis 不可用或记录已清空）");
    }

    // ---- Part B: 幂等记录（mq:idempotent:*）— 运行时幂等锁 ----
    Console.WriteLine("\n--- Part B: 幂等记录（Redis 幂等锁） ---");

    var idempKeys = new List<RedisKey>();
    foreach (var key in server.Keys(pattern: "mq:idempotent:*"))
    {
        idempKeys.Add(key);
    }

    Console.WriteLine($"Redis 中幂等记录总数：{idempKeys.Count}");

    if (idempKeys.Count > 0)
    {
        Console.WriteLine("\n--- 幂等记录明细 ---");
        foreach (var key in idempKeys.OrderBy(k => k.ToString()))
        {
            var val = await db.StringGetAsync(key);
            Console.WriteLine($"  {key} => {val}");
        }

        // 抽样验证（取当前运行的第一条消息，或幂等记录中的第一条）
        string sampleId = firstMsgId ?? idempKeys.First().ToString()!.Replace("mq:idempotent:", "");
        var sampleKey = (RedisKey)$"mq:idempotent:{sampleId}";
        var sampleVal = await db.StringGetAsync(sampleKey);
        if (sampleVal.HasValue && sampleVal.ToString().Contains("true"))
        {
            Console.WriteLine($"\n✅ 验证通过：{sampleId} 已标记 Finished=true（幂等生效）");
        }
        else
        {
            Console.WriteLine($"\n⚠️  {sampleId} 记录不存在或未完成（可能业务未处理到该消息）");
        }

        int finishedCount = 0;
        foreach (var key in idempKeys)
        {
            var val = await db.StringGetAsync(key);
            if (val.HasValue && val.ToString().Contains("true"))
                finishedCount++;
        }
        Console.WriteLine($"\n📊 已标记 Finished=true 的记录：{finishedCount}/{idempKeys.Count}");
    }
    else
    {
        Console.WriteLine("\n⚠️  Redis 中没有找到幂等记录（可能已过期）");
    }

    redis.Dispose();
}
catch (Exception ex)
{
    Console.WriteLine($"\nRedis 验证异常：{ex.GetType().Name}: {ex.Message}");
}

// ================================================================
// Block 7: 验证死信队列消费结果
// ================================================================
Console.WriteLine("\n========== Block7: 死信队列消费验证 ==========");

if (dlqMsgCount > 0)
{
    Console.WriteLine($"✅ 死信队列共消费 {dlqMsgCount} 条消息");
    Console.WriteLine("   验证路径：业务失败 → Nack requeue → 多次重试 →");
    Console.WriteLine("             x-message-ttl(8000ms) 超时 → 转发到 DLQ →");
    Console.WriteLine("             死信消费者接收并处理");
    Console.WriteLine("\n✅ 死信队列功能正常：消费失败的消息最终得到兜底处理");
}
else
{
    Console.WriteLine("ℹ️  本次运行无死信消息产生（所有消息随机模拟均成功，或重复消息被幂等拦截）");
    Console.WriteLine("   可重新运行观察：1/3 概率的业务失败会触发死信流程");
}

Console.WriteLine("\n========== 演示结束 ==========");
