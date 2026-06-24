using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqDemo.Entities;
using RabbitMqDemo.Interfaces;
using System.Text.Json;

namespace RabbitMqDemo.Services
{
    /// <summary>
    /// RabbitMQ 助手类：封装连接管理、消息发布（Confirm 模式）、消费（手动 ACK + 幂等 + 死信）
    /// </summary>
    public class RabbitMqHelper : IRabbitMqHelper
    {
        // ========== 私有字段 ==========
        private readonly IConfiguration _config;              // 配置源（读取 appsettings.json）
        private readonly IMsgIdempotentService _idempotentSvc; // Redis 幂等服务
        private readonly IMsgRecordService _msgRecordSvc;      // 消息生命周期记录服务
        private IConnection? _conn;                           // RabbitMQ 连接（自动恢复启用）
        private IModel? _publishChannel;                      // 发布专用通道（Confirm 模式）
        private IModel? _consumeChannel;                      // 消费专用通道（手动 ACK）
        private IModel? _dlxChannel;                          // 死信队列消费通道
        private readonly string _normalEx, _normalQueue;      // 普通交换机/队列名
        private readonly string _dlxEx, _dlxQueue;            // 死信交换机/队列名
        private readonly int _globalTtlMs;                    // 全局消息 TTL（毫秒）
        private bool _disposed;

        /// <summary>
        /// 构造函数：读取配置 → 建立连接 → 创建发布通道
        /// </summary>
        public RabbitMqHelper(IConfiguration config, IMsgIdempotentService idempotentSvc, IMsgRecordService msgRecordSvc)
        {
            _config = config;
            _idempotentSvc = idempotentSvc;
            _msgRecordSvc = msgRecordSvc;

            // 从 MqBusinessConfig 节读取交换机/队列/TTL 配置
            var mqBizCfg = _config.GetSection("MqBusinessConfig");
            _normalEx = mqBizCfg["NormalExchange"]!;
            _normalQueue = mqBizCfg["NormalQueue"]!;
            _dlxEx = mqBizCfg["DlxExchange"]!;
            _dlxQueue = mqBizCfg["DlxQueue"]!;
            _globalTtlMs = int.Parse(mqBizCfg["MessageTtlMs"]!);

            BuildConnection();
            BuildConfirmPublishChannel();
        }

        // ========== 连接管理 ==========

        /// <summary>
        /// 建立 RabbitMQ 连接（自动恢复启用，断线 2 秒后重试）
        /// </summary>
        private void BuildConnection()
        {
            var mqCfg = _config.GetSection("RabbitConfig");
            var factory = new ConnectionFactory
            {
                HostName = mqCfg["Host"],
                Port = int.Parse(mqCfg["Port"]!),
                UserName = mqCfg["UserName"],
                Password = mqCfg["Password"],
                VirtualHost = mqCfg["VirtualHost"],
                AutomaticRecoveryEnabled = true,           // 网络闪断自动恢复
                NetworkRecoveryInterval = TimeSpan.FromSeconds(2)
            };
            _conn = factory.CreateConnection();
            // 监听连接关闭事件（便于排查网络问题）
            _conn.ConnectionShutdown += (_, args) =>
                Console.WriteLine("[连接关闭] " + args.ReplyText);
        }

        /// <summary>
        /// 创建发布确认通道（Publisher Confirm 模式）
        /// </summary>
        private void BuildConfirmPublishChannel()
        {
            _publishChannel = _conn!.CreateModel();
            _publishChannel.ConfirmSelect(); // 开启发布确认
        }

        // ========== 队列/交换机初始化 ==========

        /// <summary>
        /// 初始化 TTL 延迟队列 + 死信队列绑定
        /// 说明：
        ///   - 普通队列：消息 TTL 过期后自动转发到死信交换机
        ///   - 死信队列：存储过期或消费失败的消息（可另起消费者处理）
        /// 幂等性：该方法可重复调用，队列已存在时不会报错
        /// </summary>
        public void InitDelayDlxQueue()
        {
            var channel = _publishChannel!;

            // 普通队列参数：绑定死信交换机 + 全局 TTL
            var normalArgs = new Dictionary<string, object>
            {
                ["x-dead-letter-exchange"] = _dlxEx,          // 死信交换机
                ["x-dead-letter-routing-key"] = _dlxQueue,    // 死信路由键
                ["x-message-ttl"] = _globalTtlMs              // 消息过期时间(ms)
            };

            // 声明 Direct 交换机（持久化，重启不丢失）
            channel.ExchangeDeclare(_normalEx, ExchangeType.Direct, durable: true);
            channel.ExchangeDeclare(_dlxEx, ExchangeType.Direct, durable: true);

            // 声明普通队列（持久化、带 TTL/死信参数）并绑定
            channel.QueueDeclare(_normalQueue, durable: true, exclusive: false, autoDelete: false, arguments: normalArgs);
            channel.QueueBind(_normalQueue, _normalEx, _normalQueue);

            // 声明死信队列（持久化）并绑定
            channel.QueueDeclare(_dlxQueue, durable: true, exclusive: false, autoDelete: false);
            channel.QueueBind(_dlxQueue, _dlxEx, _dlxQueue);

            Console.WriteLine("TTL/DLX 队列初始化完成");
        }

        // ========== 消息发布（Confirm 模式） ==========

        /// <summary>
        /// 发布消息并等待 Broker 确认
        /// </summary>
        /// <param name="msg">消息体（MsgId 用于幂等）</param>
        /// <param name="customTtlMs">可选：覆盖队列级别的消息 TTL</param>
        public async Task PublishConfirmAsync(MqMessage msg, int? customTtlMs = null)
        {
            // 通道关闭时自动重建
            if (_publishChannel is null || !_publishChannel.IsOpen)
                BuildConfirmPublishChannel();

            // 序列化为 UTF-8 字节（直接写协议层，避免字符串编解码开销）
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(msg);
            var props = _publishChannel!.CreateBasicProperties();
            props.DeliveryMode = 2; // 持久化消息（重启不丢）

            // 可选：设置消息级 TTL（覆盖队列的 x-message-ttl）
            if (customTtlMs.HasValue)
                props.Expiration = customTtlMs.Value.ToString();

            // 发布到普通交换机，路由键 = 队列名
            _publishChannel.BasicPublish(_normalEx, _normalQueue, basicProperties: props, body: body);

            // 同步等待 Broker 确认（超时 5 秒），确保消息已到达服务端
            // 使用 Task.Run 避免阻塞 async 调用方线程
            await Task.Run(() => _publishChannel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5)));

            // 持久化消息记录到 Redis（记录失败不影响消息本身）
            try { await _msgRecordSvc.CreateMessageRecordAsync(msg); }
            catch { Console.WriteLine($"[记录] {msg.MsgId} 持久化失败（Redis 不可用）"); }

            Console.WriteLine($"[消息已确认送达] {msg.MsgId}");
        }

        // ========== 消息消费 ==========

        /// <summary>
        /// 启动常驻消费者
        /// 特性：
        ///   - 手动 ACK（autoAck=false）
        ///   - 公平分发（prefetchCount=1）
        ///   - Redis 幂等去重
        ///   - 业务失败 Nack 重试（超时进入死信）
        ///   - 异常 Nack 不入队（直接进死信）
        /// </summary>
        /// <param name="businessHandler">业务处理委托（true=成功, false=失败重试）</param>
        /// <param name="cancellationToken">取消令牌（用于优雅关闭）</param>
        public async Task StartConsumeAsync(Func<MqMessage, Task<bool>> businessHandler, CancellationToken cancellationToken = default)
        {
            // 创建消费通道，一次只取一条（公平分发，防止堆积到某个消费者）
            _consumeChannel = _conn!.CreateModel();
            _consumeChannel.BasicQos(0, 1, false);

            // 使用同步 EventingBasicConsumer（回调在 IO 线程执行）
            // 注意：业务处理约 150ms << 心跳超时 60s，不会触发超时断连
            //       若业务耗时较长，应改用 AsyncEventingBasicConsumer
            var consumer = new EventingBasicConsumer(_consumeChannel);
            consumer.Received += (_, ea) =>
            {
                var tag = ea.DeliveryTag; // 投递标签，用于 ACK/Nack
                try
                {
                    // ---- 1. 反序列化 ----
                    var json = System.Text.Encoding.UTF8.GetString(ea.Body.ToArray());
                    var msg = JsonSerializer.Deserialize<MqMessage>(json);
                    if (msg == null)
                    {
                        Console.WriteLine("[反序列化失败] Nack");
                        _consumeChannel.BasicNack(tag, false, false);
                        return;
                    }

                    Console.WriteLine($"\n[收到消息] MsgId={msg.MsgId} Data={msg.Data}");

                    // ---- 2. 幂等校验（Redis SETNX + Lua 原子操作） ----
                    // 过期时间 = 队列 TTL + 30 秒，保证消息未过期时幂等记录不被提前删除
                    int expireSec = _globalTtlMs / 1000 + 30;
                    var isNew = _idempotentSvc.TryLockMsgAsync(msg.MsgId, expireSec)
                        .GetAwaiter().GetResult();
                    if (!isNew)
                    {
                        // Redis 中已存在 Finished=true 的记录 → 跳过处理
                        Console.WriteLine($"[幂等拦截] {msg.MsgId} 重复，ACK跳过");
                        // 记录：幂等跳过
                        try { _msgRecordSvc.UpdateStatusAsync(msg.MsgId, MessageStatuses.IdempotentSkipped).GetAwaiter().GetResult(); } catch { }
                        _consumeChannel.BasicAck(tag, false);
                        return;
                    }

                    // ---- 3. 业务处理 ----
                    var success = businessHandler(msg).GetAwaiter().GetResult();
                    if (success)
                    {
                        // 业务成功 → 标记幂等完成 + ACK 确认
                        _idempotentSvc.MarkFinishAsync(msg.MsgId).GetAwaiter().GetResult();
                        // 记录：已消费
                        try { _msgRecordSvc.UpdateStatusAsync(msg.MsgId, MessageStatuses.Consumed).GetAwaiter().GetResult(); } catch { }
                        _consumeChannel.BasicAck(tag, false);
                        Console.WriteLine($"[消费成功] {msg.MsgId}");
                    }
                    else
                    {
                        // 业务失败 → 释放幂等锁 + Nack 重新入队（超时后进死信）
                        _idempotentSvc.UnLockMsgAsync(msg.MsgId).GetAwaiter().GetResult();
                        // 记录：消费失败 + 累加重试次数
                        int currentRetry = 0;
                        try
                        {
                            var rec = _msgRecordSvc.GetMessageRecordAsync(msg.MsgId).GetAwaiter().GetResult();
                            currentRetry = (rec?.RetryCount ?? 0) + 1;
                            _msgRecordSvc.UpdateStatusAsync(msg.MsgId, MessageStatuses.ConsumerFailed, currentRetry).GetAwaiter().GetResult();
                        }
                        catch { }
                        _consumeChannel.BasicNack(tag, false, requeue: true);
                        Console.WriteLine($"[消费失败] {msg.MsgId} Nack重试 (第{currentRetry}次)");
                    }
                }
                catch (Exception ex)
                {
                    // ---- 4. 异常兜底 ----
                    Console.WriteLine($"[消费异常] tag={tag} {ex.GetType().Name}: {ex.Message}");
                    if (ex is StackExchange.Redis.RedisConnectionException)
                        Console.WriteLine($"[Redis异常] 请检查Redis: {_config["RedisConfig:ConnStr"]}");
                    // Nack 不重新入队，避免无限循环卡死消费者
                    try { _consumeChannel.BasicNack(tag, false, false); } catch { }
                }
            };

            // 注册消费者（autoAck=false：手动确认）
            _consumeChannel.BasicConsume(_normalQueue, autoAck: false, consumer: consumer);
            Console.WriteLine("消费者启动完成，等待消息...");

            // 长时间阻塞，直到外部传入取消令牌（Ctrl+C 或超时）
            try
            {
                await Task.Delay(-1, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\n消费者收到关闭信号，正在退出...");
            }
        }

        // ========== 死信队列消费 ==========

        /// <summary>
        /// 启动死信队列消费者
        ///
        /// 消费流程：
        ///   1. 从死信队列接收消息（源自 Nack 重试超时的业务消息）
        ///   2. 反序列化并调用外部处理委托
        ///   3. 处理成功 Ack 移除，失败 Nack 丢弃（不进二次死信）
        /// </summary>
        /// <param name="businessHandler">可选死信处理委托（null=仅记录）, true=确认删除, false=拒绝丢弃</param>
        /// <param name="cancellationToken">取消令牌</param>
        public async Task StartDlxConsumeAsync(Func<MqMessage, Task<bool>>? businessHandler = null, CancellationToken cancellationToken = default)
        {
            _dlxChannel = _conn!.CreateModel();
            _dlxChannel.BasicQos(0, 1, false);

            var consumer = new EventingBasicConsumer(_dlxChannel);
            consumer.Received += (_, ea) =>
            {
                var tag = ea.DeliveryTag;
                try
                {
                    // ---- 1. 反序列化 ----
                    var json = System.Text.Encoding.UTF8.GetString(ea.Body.ToArray());
                    var msg = JsonSerializer.Deserialize<MqMessage>(json);
                    if (msg == null)
                    {
                        Console.WriteLine("[DLQ] 反序列化失败，Nack丢弃");
                        _dlxChannel.BasicNack(tag, false, false);
                        return;
                    }

                    Console.WriteLine($"\n[DLQ→收到死信] MsgId={msg.MsgId} Data={msg.Data} CreateTs={msg.CreateTs}");

                    // 提取死信原因头（x-death），显示原始路由和重试次数
                    if (ea.BasicProperties?.Headers != null &&
                        ea.BasicProperties.Headers.TryGetValue("x-death", out var xDeathRaw))
                    {
                        try
                        {
                            // x-death 是嵌套列表，取第一条简要信息
                            var deathList = xDeathRaw as List<object>;
                            if (deathList?.Count > 0)
                            {
                                var firstDeath = deathList[0] as Dictionary<string, object>;
                                if (firstDeath != null)
                                {
                                    var reason = firstDeath.TryGetValue("reason", out var r) ? r : "expired";
                                    var count = firstDeath.TryGetValue("count", out var c) ? c : "?";
                                    var q = firstDeath.TryGetValue("queue", out var qq) ? qq : "?";
                                    Console.WriteLine($"  [死信原因] reason={reason} | 原队列={q} | 重试次数={count}");
                                }
                            }
                        }
                        catch { /* 头信息解析失败不影响主流程 */ }
                    }

                    // 记录：已进死信（在执行业务处理前记录，确保死信事实不被遗漏）
                    try
                    {
                        var drec = _msgRecordSvc.GetMessageRecordAsync(msg.MsgId).GetAwaiter().GetResult();
                        int dlqRetry = drec?.RetryCount ?? 0;
                        _msgRecordSvc.UpdateStatusAsync(msg.MsgId, MessageStatuses.DeadLettered, dlqRetry).GetAwaiter().GetResult();
                    }
                    catch { }

                    // ---- 2. 执行业务处理 ----
                    bool success = true;
                    if (businessHandler != null)
                    {
                        success = businessHandler(msg).GetAwaiter().GetResult();
                    }

                    if (success)
                    {
                        _dlxChannel.BasicAck(tag, false);
                        Console.WriteLine($"[DLQ✓] {msg.MsgId} 死信处理完成，已确认");
                    }
                    else
                    {
                        _dlxChannel.BasicNack(tag, false, false);
                        Console.WriteLine($"[DLQ✗] {msg.MsgId} 死信处理失败，已丢弃");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DLQ异常] tag={tag} {ex.GetType().Name}: {ex.Message}");
                    try { _dlxChannel.BasicNack(tag, false, false); } catch { }
                }
            };

            // 注册消费者（autoAck=false：手动确认）
            _dlxChannel.BasicConsume(_dlxQueue, autoAck: false, consumer: consumer);
            Console.WriteLine("死信队列消费者已启动，等待死信消息...\n");

            // 阻塞直到外部传入取消令牌
            try
            {
                await Task.Delay(-1, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\n死信消费者收到关闭信号，正在退出...");
            }
        }

        // ========== 资源释放 ==========

        /// <summary>
        /// 释放 RabbitMQ 连接资源（通道 → 连接）
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _publishChannel?.Close();
            _consumeChannel?.Close();
            _dlxChannel?.Close();
            _conn?.Close();
            _conn?.Dispose();
            _disposed = true;
            Console.WriteLine("\nMQ 连接已释放");
        }
    }
}
