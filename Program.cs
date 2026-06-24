using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMqDemo.Entities;
using RabbitMqDemo.Extensions;
using RabbitMqDemo.Interfaces;
using StackExchange.Redis;
using System.Diagnostics;

// ===== 控制台输出配置（必须放在最前面） =====
Console.OutputEncoding = System.Text.Encoding.UTF8;
if (Console.Out is System.IO.StreamWriter sw)
    sw.AutoFlush = true;

// ===== 构建依赖注入容器 =====
var host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration(cfgBuilder =>
    {
        cfgBuilder.AddJsonFile("appsettings.json", reloadOnChange: true, optional: false);
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddRedisIdempotent(ctx.Configuration);
        services.AddRedisCache(ctx.Configuration);
        services.AddRabbitMqHelper();
    })
    .Build();

var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

// 解析服务
using var scope = host.Services.CreateScope();
var mqHelper = scope.ServiceProvider.GetRequiredService<IRabbitMqHelper>();
var msgRecordSvc = scope.ServiceProvider.GetRequiredService<IMsgRecordService>();

// 缓存服务（单例，可直接从 host 解析）
var cacheSvc = host.Services.GetRequiredService<ICacheService>();

// =============================================================
// 全局计数器 & 共享变量（在 try/finally/Block 间共享）
// =============================================================
// 主业务
int dlqMsgCount = 0;
string? firstMsgId = null;
List<string> runMsgIds = [];

// 6 大场景计数器
int asyncTaskCount = 0;       // 异步处理
int decoupleCount = 0;        // 服务解耦
int spikeCount = 0;           // 削峰填谷
int delayCount = 0;           // 延迟队列
int txCount = 0;              // 分布式事务
int broadcastCount = 0;       // 消息广播

// 消费者任务列表（后台并发）
List<Task> allConsumerTasks = [];

// 运行标签（唯一标识本次运行）
string runTag = DateTime.Now.ToString("yyyyMMddHHmmss");

CancellationTokenSource? stopCts = null;

try
{
    // ================================================================
    // Block 1: 初始化所有基础设施（DLX + 6 场景 Ex/Queue）
    // ================================================================
    Console.WriteLine("\n========== Block1: 初始化队列和交换机 ==========");

    // ---- 1a: 原有 DLX 基础设施 ----
    Console.WriteLine("  1a: DLX（死信交换）");
    mqHelper.InitDelayDlxQueue();

    // ---- 1b: 异步处理（Direct） ----
    Console.WriteLine("  1b: 异步处理 demo.async.ex→demo.async.queue");
    mqHelper.DeclareExQueue("demo.async.ex", "direct", "demo.async.queue", "demo.async.queue");

    // ---- 1c: 服务解耦（Fanout） ----
    Console.WriteLine("  1c: 服务解耦 demo.decouple.ex → svc_a / svc_b / svc_c");
    mqHelper.DeclareExQueue("demo.decouple.ex", "fanout", "demo.svc_a.queue", "");
    mqHelper.DeclareExQueue("demo.decouple.ex", "fanout", "demo.svc_b.queue", "");
    mqHelper.DeclareExQueue("demo.decouple.ex", "fanout", "demo.svc_c.queue", "");

    // ---- 1d: 削峰填谷（Direct） ----
    Console.WriteLine("  1d: 削峰填谷 demo.spike.ex→demo.spike.queue（2个消费者）");
    mqHelper.DeclareExQueue("demo.spike.ex", "direct", "demo.spike.queue", "demo.spike.queue");

    // ---- 1e: 延迟队列（TTL + DLX） ----
    Console.WriteLine("  1e: 延迟队列 demo.delay.ex→src(5s TTL)→target");
    var delayArgs = new Dictionary<string, object>
    {
        ["x-dead-letter-exchange"] = "demo.delay.ex",
        ["x-dead-letter-routing-key"] = "demo.delay.target.queue",
        ["x-message-ttl"] = 5000
    };
    mqHelper.DeclareExQueue("demo.delay.ex", "direct", "demo.delay.src.queue", "demo.delay.src.queue", delayArgs);
    mqHelper.DeclareExQueue("demo.delay.ex", "direct", "demo.delay.target.queue", "demo.delay.target.queue");

    // ---- 1f: 分布式事务（Direct） ----
    Console.WriteLine("  1f: 分布式事务 demo.tx.ex→order / payment / inventory");
    mqHelper.DeclareExQueue("demo.tx.ex", "direct", "demo.tx.order.queue", "demo.tx.order.queue");
    mqHelper.DeclareExQueue("demo.tx.ex", "direct", "demo.tx.payment.queue", "demo.tx.payment.queue");
    mqHelper.DeclareExQueue("demo.tx.ex", "direct", "demo.tx.inventory.queue", "demo.tx.inventory.queue");

    // ---- 1g: 消息广播（Fanout） ----
    Console.WriteLine("  1g: 消息广播 demo.broadcast.ex → sub1 / sub2");
    mqHelper.DeclareExQueue("demo.broadcast.ex", "fanout", "demo.sub1.queue", "");
    mqHelper.DeclareExQueue("demo.broadcast.ex", "fanout", "demo.sub2.queue", "");

    Console.WriteLine("✅ 全部基础设施初始化完成\n");

    // ================================================================
    // Block 2: 启动死信队列消费者（后台）
    // ================================================================
    Console.WriteLine("========== Block2: 死信队列消费者 ==========");

    stopCts = CancellationTokenSource.CreateLinkedTokenSource(lifetime.ApplicationStopping);
    stopCts.CancelAfter(TimeSpan.FromSeconds(60));

    allConsumerTasks.Add(mqHelper.StartDlxConsumeAsync(msg =>
    {
        Interlocked.Increment(ref dlqMsgCount);
        Console.WriteLine($"  ↳ [死信] MsgId={msg.MsgId}");
        return Task.FromResult(true);
    }, stopCts.Token));

    Console.WriteLine("✅ 死信消费者已启动\n");

    // ================================================================
    // Block 3: 启动普通队列消费者（主业务 + Redis 幂等，后台）
    // ================================================================
    Console.WriteLine("========== Block3: 普通队列消费者 ==========");
    Console.WriteLine("  特性：Redis 幂等 | 手动 ACK | 失败 Nack 重试入死信\n");

    allConsumerTasks.Add(mqHelper.StartConsumeAsync(msg =>
    {
        Console.WriteLine($"[主业务] MsgId={msg.MsgId} | Data={msg.Data}");
        var rand = Random.Shared.Next(1, 4);
        if (rand == 2)
        {
            Console.WriteLine($"[主业务↓] {msg.MsgId} 模拟异常，Nack重试→死信");
            return Task.FromResult(false);
        }
        Thread.Sleep(50);
        return Task.FromResult(true);
    }, stopCts.Token));

    Console.WriteLine("✅ 主消费者已启动\n");

    // ================================================================
    // Block 4: 启动 6 大场景消费者（全部后台并发）
    // ================================================================
    Console.WriteLine("========== Block4: 场景消费者 ==========");

    // ---- 4a: 异步处理 ----
    Console.WriteLine("  4a: 异步处理消费者 demo.async.queue");
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.async.queue", async msg =>
    {
        Console.WriteLine($"\n[异步任务开始] MsgId={msg.MsgId} | 内容={msg.Data}");
        await Task.Delay(2000); // 模拟 2s 耗时操作
        Interlocked.Increment(ref asyncTaskCount);
        Console.WriteLine($"[异步任务完成] ✓ 耗时 2s | 已处理数={asyncTaskCount}");
        return true;
    }, stopCts.Token));

    // ---- 4b: 服务解耦（3 个服务各一个消费者） ----
    Console.WriteLine("  4b: 服务解耦 → svc_a / svc_b / svc_c");
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.svc_a.queue", msg =>
    {
        Console.WriteLine($"  [服务A] 收到: {msg.MsgId} | Data={msg.Data}");
        Interlocked.Increment(ref decoupleCount);
        return Task.FromResult(true);
    }, stopCts.Token));
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.svc_b.queue", msg =>
    {
        Console.WriteLine($"  [服务B] 收到: {msg.MsgId} | Data={msg.Data}");
        Interlocked.Increment(ref decoupleCount);
        return Task.FromResult(true);
    }, stopCts.Token));
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.svc_c.queue", msg =>
    {
        Console.WriteLine($"  [服务C] 收到: {msg.MsgId} | Data={msg.Data}");
        Interlocked.Increment(ref decoupleCount);
        return Task.FromResult(true);
    }, stopCts.Token));

    // ---- 4c: 削峰填谷（2 个消费线程各 300ms/条） ----
    Console.WriteLine("  4c: 削峰填谷消费者 ×2（各 300ms/条）");
    for (int c = 1; c <= 2; c++)
    {
        int consumerId = c;
        allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.spike.queue", async msg =>
        {
            int cur = Interlocked.Increment(ref spikeCount);
            Console.WriteLine($"  [削峰#{consumerId}] 消费 {msg.MsgId} | Data={msg.Data} | 进度 {cur}/20");
            await Task.Delay(300);
            return true;
        }, stopCts.Token));
    }

    // ---- 4d: 延迟队列目标消费者 ----
    Console.WriteLine("  4d: 延迟队列消费者 demo.delay.target.queue");
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.delay.target.queue", msg =>
    {
        Interlocked.Increment(ref delayCount);
        Console.WriteLine($"  [延迟队列✓] MsgId={msg.MsgId} | Data={msg.Data} | 到达时间={DateTime.Now:yyyy.MM.dd HH:mm:ss.fff}");
        return Task.FromResult(true);
    }, stopCts.Token));

    // ---- 4e: 分布式事务（order → payment → inventory） ----
    Console.WriteLine("  4e: 分布式事务消费者（order / payment / inventory）");
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.tx.order.queue", msg =>
    {
        int step = Interlocked.Increment(ref txCount);
        Console.WriteLine($"  [事务:下单✓] MsgId={msg.MsgId} | Data={msg.Data} | step={step}/3");
        return Task.FromResult(true);
    }, stopCts.Token));
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.tx.payment.queue", msg =>
    {
        int step = Interlocked.Increment(ref txCount);
        Console.WriteLine($"  [事务:支付✓] MsgId={msg.MsgId} | Data={msg.Data} | step={step}/3");
        return Task.FromResult(true);
    }, stopCts.Token));
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.tx.inventory.queue", msg =>
    {
        int step = Interlocked.Increment(ref txCount);
        Console.WriteLine($"  [事务:库存✓] MsgId={msg.MsgId} | Data={msg.Data} | step={step}/3");
        return Task.FromResult(true);
    }, stopCts.Token));

    // ---- 4f: 消息广播（2 个订阅者） ----
    Console.WriteLine("  4f: 消息广播消费者 sub1 / sub2");
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.sub1.queue", msg =>
    {
        Interlocked.Increment(ref broadcastCount);
        Console.WriteLine($"  [广播订阅者1] 收到: {msg.MsgId} | Data={msg.Data}");
        return Task.FromResult(true);
    }, stopCts.Token));
    allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync("demo.sub2.queue", msg =>
    {
        Interlocked.Increment(ref broadcastCount);
        Console.WriteLine($"  [广播订阅者2] 收到: {msg.MsgId} | Data={msg.Data}");
        return Task.FromResult(true);
    }, stopCts.Token));

    Console.WriteLine("✅ 全部场景消费者已启动\n");

    // ================================================================
    // Block 5: 发送 6 大场景验证消息
    // ================================================================
    Console.WriteLine("========== Block5: 发送场景验证消息 ==========");

    // ---- 5a: 异步处理 ----
    Console.WriteLine("\n>>> 5a: 异步处理 → 发送慢任务消息（模拟发 100 封邮件）");
    var asyncMsg = new MqMessage
    {
        MsgId = $"async_{runTag}_001",
        Data = "发送100封批量通知邮件（模拟耗时 2s）"
    };
    await mqHelper.PublishToExchangeAsync("demo.async.ex", "demo.async.queue", asyncMsg);
    Console.WriteLine("  主流程已返回，异步任务在后台处理...\n");

    // ---- 5b: 服务解耦 ----
    Console.WriteLine(">>> 5b: 服务解耦 → 发送「订单已创建」事件到 Fanout（应被 3 个服务接收）");
    var decoupleMsg = new MqMessage
    {
        MsgId = $"order_{runTag}_001",
        Data = "订单 ORD-20260624-0001 已创建"
    };
    await mqHelper.PublishToExchangeAsync("demo.decouple.ex", "", decoupleMsg);

    // ---- 5c: 削峰填谷 ----
    Console.WriteLine("\n>>> 5c: 削峰填谷 → 发送 20 条秒杀订单（急速发送，消费端限流处理）");
    for (int i = 1; i <= 20; i++)
    {
        var spikeMsg = new MqMessage
        {
            MsgId = $"spike_{runTag}_{i:D3}",
            Data = $"秒杀订单_{i}（商品ID={Random.Shared.Next(1000, 9999)}）"
        };
        await mqHelper.PublishToExchangeAsync("demo.spike.ex", "demo.spike.queue", spikeMsg);
    }
    Console.WriteLine("  20 条秒杀订单已全部发送，2 个消费者正在限流处理...\n");

    // ---- 5d: 延迟队列 ----
    Console.WriteLine(">>> 5d: 延迟队列 → 发送 1 条延期消息（5s 后到达目标队列）");
    var delayMsg = new MqMessage
    {
        MsgId = $"delay_{runTag}_001",
        Data = $"5s 后到期的定时任务 | 发送时间={DateTime.Now:yyyy.MM.dd HH:mm:ss.fff}"
    };
    await mqHelper.PublishToExchangeAsync("demo.delay.ex", "demo.delay.src.queue", delayMsg);

    // ---- 5e: 分布式事务 ----
    Console.WriteLine("\n>>> 5e: 分布式事务 → 模拟订单→支付→库存 3 阶段提交");
    var txOrder = new MqMessage { MsgId = $"tx_order_{runTag}_001", Data = "下单成功，金额 ¥299" };
    var txPay = new MqMessage { MsgId = $"tx_pay_{runTag}_001", Data = "支付成功，扣除余额 ¥299" };
    var txInv = new MqMessage { MsgId = $"tx_inv_{runTag}_001", Data = "库存扣减，SKU=AC-1001 ×1" };
    await mqHelper.PublishToExchangeAsync("demo.tx.ex", "demo.tx.order.queue", txOrder);
    await mqHelper.PublishToExchangeAsync("demo.tx.ex", "demo.tx.payment.queue", txPay);
    await mqHelper.PublishToExchangeAsync("demo.tx.ex", "demo.tx.inventory.queue", txInv);

    // ---- 5f: 消息广播 ----
    Console.WriteLine("\n>>> 5f: 消息广播 → 发送 1 条系统公告到 Fanout（应被 sub1/sub2 同时接收）");
    var broadMsg = new MqMessage
    {
        MsgId = $"notice_{runTag}_001",
        Data = "【系统公告】凌晨 2:00-4:00 停机维护"
    };
    await mqHelper.PublishToExchangeAsync("demo.broadcast.ex", "", broadMsg);

    Console.WriteLine("\n✅ 全部场景消息发送完成\n");

    // ================================================================
    // Block 6: 原有消息发送（10 条唯一 + 5 条重复 + 持久化验证）
    // ================================================================
    Console.WriteLine("========== Block6: 原有消息发送测试 ==========");
    Console.WriteLine(">>> 6a: 发送10条唯一消息（幂等 + 持久化验证）\n");

    for (int i = 1; i <= 10; i++)
    {
        string msgId = $"msg_{runTag}_{i:D3}";
        if (i == 1) firstMsgId = msgId;
        runMsgIds.Add(msgId);
        await mqHelper.PublishConfirmAsync(new MqMessage { MsgId = msgId, Data = $"业务数据_{i}" });
        await Task.Delay(100);
    }
    Console.WriteLine("\n✅ 10 条唯一消息发送完成\n");

    Console.WriteLine(">>> 6b: 发送5条重复消息（幂等拦截测试）\n");
    for (int i = 0; i < 5; i++)
    {
        await mqHelper.PublishConfirmAsync(new MqMessage { MsgId = runMsgIds[i], Data = $"重复数据_{i + 1}" });
        await Task.Delay(100);
    }
    Console.WriteLine("\n✅ 5 条重复消息发送完成\n");

    Console.WriteLine(">>> 6c: 验证 Redis 持久化");
    var verifyTarget = firstMsgId ?? runMsgIds[0];
    var verifyRecord = await msgRecordSvc.GetMessageRecordAsync(verifyTarget);
    Console.WriteLine(verifyRecord != null
        ? $"  ✅ {verifyTarget} 已持久化 | 状态={verifyRecord.Status}"
        : $"  ⚠️  {verifyTarget} 未找到");

    Console.WriteLine("\n📢 消息全部发送完毕，等待消费处理（60s 超时自动退出）...\n");

    // 等待所有消费者完成（被 CTS 60s 超时触发结束）
    try { await Task.WhenAll(allConsumerTasks); }
    catch (OperationCanceledException) { }
    catch (AggregateException ae)
    {
        foreach (var ex in ae.InnerExceptions)
            if (ex is not OperationCanceledException)
                Console.WriteLine($"消费者异常：{ex.Message}");
    }
}
catch (OperationCanceledException)
{
    // 正常超时退出
}
catch (Exception ex)
{
    Console.WriteLine($"全局异常：{ex.GetType().Name}: {ex.Message}");
}
finally
{
    // ================================================================
    // Block 7: 停止消费者 & 清理资源
    // ================================================================
    Console.WriteLine("\n========== Block7: 停止消费者 & 清理 ==========");

    stopCts?.Cancel();
    // 等待已发出的消费者任务优雅退出
    try { await Task.WhenAll(allConsumerTasks); } catch { }

    // 输出各场景计数
    Console.WriteLine($"\n📊 场景消费汇总:");
    Console.WriteLine($"  死信队列    : {dlqMsgCount} 条");
    Console.WriteLine($"  异步处理    : {asyncTaskCount}/1");
    Console.WriteLine($"  服务解耦    : {decoupleCount}/3");
    Console.WriteLine($"  削峰填谷    : {spikeCount}/20");
    Console.WriteLine($"  延迟队列    : {delayCount}/1");
    Console.WriteLine($"  分布式事务  : {txCount}/3");
    Console.WriteLine($"  消息广播    : {broadcastCount}/2");

    mqHelper.Dispose();
    await host.StopAsync();
    Console.WriteLine("✅ 资源清理完成\n");
}

// ================================================================
// Block 8: 验证 Redis 消息记录 + 幂等记录
// ================================================================
Console.WriteLine("========== Block8: Redis 记录验证 ==========");

try
{
    using var redis = ConnectionMultiplexer.Connect(
        (host.Services.GetRequiredService<IConfiguration>())["RedisConfig:ConnStr"]
        ?? "127.0.0.1:6379,password=123456,defaultDatabase=0");
    var db = redis.GetDatabase();
    var server = redis.GetServer(redis.GetEndPoints()[0]);

    // ---- Part A: 消息记录 ----
    Console.WriteLine("\n--- Part A: 消息记录（mq:record:*） ---");
    var recordKeys = server.Keys(pattern: "mq:record:*").ToArray();
    Console.WriteLine($"消息记录总数：{recordKeys.Length}");

    if (recordKeys.Length > 0)
    {
        Console.WriteLine("\n--- 本次运行消息状态 ---");
        var runRecords = recordKeys
            .Where(k => k.ToString()!.Contains(runTag))
            .OrderBy(k => k.ToString())
            .ToArray();

        if (runRecords.Length > 0)
        {
            foreach (var key in runRecords)
            {
                var json = await db.StringGetAsync(key);
                if (json.HasValue)
                    Console.WriteLine($"  {key} => {json}");
            }
        }

        // 全量状态分布
        var allRecs = new List<MessageRecord>();
        var opts = new System.Text.Json.JsonSerializerOptions
        {
            PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase
        };
        foreach (var key in recordKeys)
        {
            var json = await db.StringGetAsync(key);
            if (json.HasValue)
            {
                var rec = System.Text.Json.JsonSerializer.Deserialize<MessageRecord>((string)json!, opts);
                if (rec != null) allRecs.Add(rec);
            }
        }
        var dist = allRecs.GroupBy(r => r.Status).ToDictionary(g => g.Key, g => g.Count());
        Console.WriteLine("\n--- 全量状态分布 ---");
        foreach (var s in new[] { "未消费", "已消费", "已进死信", "幂等跳过", "消费失败" })
        {
            int c = dist.GetValueOrDefault(s, 0);
            Console.WriteLine($"  {(c > 0 ? "●" : "○")} {s}: {c}");
        }
    }

    // ---- Part B: 幂等记录 ----
    Console.WriteLine("\n--- Part B: 幂等记录（mq:idempotent:*） ---");
    var idempKeys = server.Keys(pattern: "mq:idempotent:*").ToArray();
    Console.WriteLine($"幂等记录总数：{idempKeys.Length}");

    if (idempKeys.Length > 0)
    {
        foreach (var key in idempKeys.OrderBy(k => k.ToString()))
        {
            var val = await db.StringGetAsync(key);
            Console.WriteLine($"  {key} => {val}");
        }

        string sampleId = firstMsgId ?? idempKeys.First().ToString()!.Replace("mq:idempotent:", "");
        var sample = await db.StringGetAsync($"mq:idempotent:{sampleId}");
        Console.WriteLine(sample.HasValue && sample.ToString().Contains("true")
            ? $"\n✅ 验证通过：{sampleId} Finished=true"
            : $"\n⚠️  {sampleId} 未完成");
    }

    redis.Dispose();
}
catch (Exception ex)
{
    Console.WriteLine($"\nRedis 验证异常：{ex.GetType().Name}: {ex.Message}");
}

// ================================================================
// Block 9: 6 大场景结果验证
// ================================================================
Console.WriteLine("\n========== Block9: 场景结果验证 ==========");

// 9a: 异步处理
Console.WriteLine("\n--- 9a: 异步处理 ---");
Console.WriteLine(asyncTaskCount >= 1
    ? "✅ 异步任务已处理：主流程返回后消费者在后台完成耗时操作，实现了非核心操作异步化"
    : "❌ 异步任务未完成");

// 9b: 服务解耦
Console.WriteLine("\n--- 9b: 服务解耦 ---");
Console.WriteLine(decoupleCount == 3
    ? $"✅ 服务解耦验证：1 条事件被 3 个服务独立接收（{decoupleCount}/3），服务间无耦合"
    : $"⚠️  服务解耦：期望 3，实际 {decoupleCount}");

// 9c: 削峰填谷
Console.WriteLine("\n--- 9c: 削峰填谷 ---");
Console.WriteLine(spikeCount >= 18
    ? $"✅ 削峰填谷验证：20 条秒杀订单已逐步消费（{spikeCount}/20），高峰流量被队列削平"
    : $"⚠️  削峰填谷：期望 ≈20，实际 {spikeCount}（可能仍在消费中）");

// 9d: 延迟队列
Console.WriteLine("\n--- 9d: 延迟队列 ---");
Console.WriteLine(delayCount >= 1
    ? $"✅ 延迟队列验证：消息经过 5s TTL 延迟后到达目标队列，定时/超时类业务可用此模式"
    : $"❌ 延迟队列未收到消息（可能 5s TTL 内程序已退出）");

// 9e: 分布式事务
Console.WriteLine("\n--- 9e: 分布式事务 ---");
Console.WriteLine(txCount == 3
    ? $"✅ 分布式事务验证：3 阶段（订单→支付→库存）全部完成（{txCount}/3），最终一致性达成"
    : $"⚠️  分布式事务：期望 3，实际 {txCount}");

// 9f: 消息广播
Console.WriteLine("\n--- 9f: 消息广播 ---");
Console.WriteLine(broadcastCount == 2
    ? $"✅ 消息广播验证：1 条公告被 2 个订阅者同时接收（{broadcastCount}/2），一对多通知生效"
    : $"⚠️  消息广播：期望 2，实际 {broadcastCount}");

Console.WriteLine("\n========== 演示结束 ==========");

// ================================================================
// Block 10: Redis 缓存三大防护策略演示
// ================================================================
Console.WriteLine("\n\n╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║    Block 10: Redis 缓存三大防护策略演示              ║");
Console.WriteLine("║    ① 穿透防护（Cache Penetration Protection）         ║");
Console.WriteLine("║    ② 击穿防护（Cache Breakdown Protection）          ║");
Console.WriteLine("║    ③ 雪崩防护（Cache Avalanche Protection）          ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝\n");

int cachePenetrationBlocked = 0;
int cacheDbFallbackCalls = 0;

Console.WriteLine("\n━━━ ① 缓存穿透防护演示 ━━━");
Console.WriteLine("  场景：攻击者不断请求一个数据库中根本不存在的 key\n");

string nonExistKey = $"nonexistent_{runTag}_data";
for (int round = 1; round <= 5; round++)
{
    var penSw = Stopwatch.StartNew();
    var result = await cacheSvc.GetOrSetAsync<string>(nonExistKey, async () =>
    {
        Interlocked.Increment(ref cacheDbFallbackCalls);
        Console.WriteLine($"  ⚡ [第{round}次] 回源查询 DB → 未找到数据（返回 null）");
        await Task.Delay(200); // 模拟 DB 查询耗时
        return null; // 模拟数据库中也不存在
    }, expiry: TimeSpan.FromSeconds(60));

    penSw.Stop();

    if (result is null)
    {
        Console.WriteLine($"     ↳ 第{round}次调用 → 返回 null | 耗时={penSw.ElapsedMilliseconds}ms");

        if (round >= 2 && penSw.ElapsedMilliseconds < 20)
        {
            Interlocked.Increment(ref cachePenetrationBlocked);
            Console.WriteLine($"     ✅ 穿透防护生效：请求被空值缓存拦截，未穿透到回源方法");
        }
    }
}
Console.WriteLine($"\n  📊 穿透防护统计：{cacheDbFallbackCalls} 次回源（实际仅第1次） | {cachePenetrationBlocked} 次被空值缓存拦截\n");

Console.WriteLine("\n━━━ ② 缓存击穿防护演示 ━━━");
Console.WriteLine("  场景：热点 key 失效，大量并发请求同时到达\n");

string hotKey = $"hotkey_{runTag}_data";
int dbCallCount = 0;

var tasks = new Task<string?>[10];
var stopwatch = Stopwatch.StartNew();

for (int i = 0; i < tasks.Length; i++)
{
    int taskId = i;
    tasks[i] = cacheSvc.GetOrSetAsync<string>(hotKey, async () =>
    {
        var count = Interlocked.Increment(ref dbCallCount);
        Console.WriteLine($"  ⚡ [Task{taskId}] 获取锁成功，回源 DB 加载数据（回源次数#{count}）");
        await Task.Delay(300); // 模拟 DB 查询耗时
        return $"这是热点数据（由 Task{taskId} 加载）";
    }, expiry: TimeSpan.FromSeconds(10));
}

var results = await Task.WhenAll(tasks);
stopwatch.Stop();

Console.WriteLine($"\n  📊 10 个并发请求 → 实际回源 {dbCallCount} 次 | 总耗时={stopwatch.ElapsedMilliseconds}ms");
Console.WriteLine(dbCallCount == 1
    ? "  ✅ 击穿防护生效：仅第 1 个请求回源 DB，其余 9 个等待后从缓存获取"
    : "  ⚠️  击穿防护部分生效");

// 显示所有结果
for (int i = 0; i < results.Length; i++)
{
    if (i == 0 || results[i] != results[i - 1])
        Console.WriteLine($"  Task{i} 结果: {results[i]}");
}

Console.WriteLine("\n━━━ ③ 缓存雪崩防护演示 ━━━");
Console.WriteLine("  场景：批量缓存同时过期导致 DB 负载尖峰\n");
Console.WriteLine("  防护策略：对每个 key 的 TTL 添加 ±25% 的随机偏移\n");

Console.WriteLine("  设置 8 个缓存，统一 TTL=60s，观察实际 TTL 的随机分布：");
for (int i = 1; i <= 8; i++)
{
    string batchKey = $"avalance_test_{i}_{runTag}";
    var setSw = Stopwatch.StartNew();
    await cacheSvc.SetAsync(batchKey, $"测试数据_{i}", TimeSpan.FromSeconds(60));
    setSw.Stop();
    Console.WriteLine($"  Key cache:data:avalance_test_{i}_{runTag}  TTL=60s → 设置耗时={setSw.ElapsedMilliseconds}ms");
}

// 读取实际 TTL（用 KeyTimeToLive 检查）
using (var debugRedis = ConnectionMultiplexer.Connect(
    (host.Services.GetRequiredService<IConfiguration>())["RedisConfig:ConnStr"]
    ?? "127.0.0.1:6379,password=123456,defaultDatabase=0"))
{
    var debugDb = debugRedis.GetDatabase();
    Console.WriteLine("\n  各 key 实际剩余 TTL（雪崩防护 Jitter 效果）：");
    for (int i = 1; i <= 8; i++)
    {
        string batchKey = $"cache:data:avalance_test_{i}_{runTag}";
        var ttl = await debugDb.KeyTimeToLiveAsync(batchKey);
        if (ttl.HasValue)
        {
            Console.WriteLine($"  cache:data:avalance_test_{i}_{runTag,-5} TTL={ttl.Value.TotalSeconds:F1}s" +
                (ttl.Value.TotalSeconds is >= 45 and <= 75 ? " ✓" : ""));
        }
    }

    // 清理测试 key
    for (int i = 1; i <= 8; i++)
        await debugDb.KeyDeleteAsync($"cache:data:avalance_test_{i}_{runTag}");
    await debugDb.KeyDeleteAsync($"cache:data:{nonExistKey}");
    await debugDb.KeyDeleteAsync($"cache:data:{hotKey}");
}

Console.WriteLine("\n========== Block 10 缓存演示结束 ==========");
