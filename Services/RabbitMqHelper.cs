using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqDemo.Entities;
using RabbitMqDemo.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace RabbitMqDemo.Services
{
    public class RabbitMqHelper : IRabbitMqHelper
    {
        #region 私有字段
        private readonly IConfiguration _config;
        private readonly IMsgIdempotentService _idempotentSvc;
        private IConnection? _conn;
        private IModel? _publishChannel; // 发布专用Confirm通道
        private IModel? _consumeChannel;   // 消费通道（手动ACK）
        private readonly string _normalEx, _normalQueue, _dlxEx, _dlxQueue;
        private readonly int _globalTtlMs;
        private bool _disposed;
        #endregion

        public RabbitMqHelper(IConfiguration config, IMsgIdempotentService idempotentSvc)
        {
            _config = config;
            _idempotentSvc = idempotentSvc;

            var mqBizCfg = _config.GetSection("MqBusinessConfig");
            _normalEx = mqBizCfg["NormalExchange"]!;
            _normalQueue = mqBizCfg["NormalQueue"]!;
            _dlxEx = mqBizCfg["DlxExchange"]!;
            _dlxQueue = mqBizCfg["DlxQueue"]!;
            _globalTtlMs = int.Parse(mqBizCfg["MessageTtlMs"]!);

            BuildConnection();
            BuildConfirmPublishChannel();
        }

        #region 连接与Confirm发布通道初始化
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
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(2)
            };
            _conn = factory.CreateConnection();
        }

        /// <summary>开启Publisher Confirms 异步回调高性能模式</summary>
        private void BuildConfirmPublishChannel()
        {
            _publishChannel = _conn!.CreateModel();
            _publishChannel.ConfirmSelect(); // 开启发布确认

            // 异步Confirm回调，不阻塞发布线程
            _publishChannel.BasicAcks += (_, args) =>
            {
                Console.WriteLine($"[Confirm成功] DeliveryTag={args.DeliveryTag}, 批量确认:{args.Multiple}");
            };
            _publishChannel.BasicNacks += (_, args) =>
            {
                Console.WriteLine($"[Confirm失败] DeliveryTag={args.DeliveryTag}, 批量确认:{args.Multiple}");
            };
        }
        #endregion

        #region TTL延迟队列 + 死信DLX绑定声明
        public void InitDelayDlxQueue()
        {
            // 普通队列参数：绑定死信交换机、全局TTL
            var normalArgs = new Dictionary<string, object>
            {
                ["x-dead-letter-exchange"] = _dlxEx,
                ["x-dead-letter-routing-key"] = _dlxQueue,
                ["x-message-ttl"] = _globalTtlMs
            };
            var normalOpt = new QueueArgModel { Args = normalArgs };
            var dlxOpt = new QueueArgModel();

            // 声明交换机
            _publishChannel!.ExchangeDeclare(_normalEx, ExchangeType.Direct, durable: true);
            _publishChannel.ExchangeDeclare(_dlxEx, ExchangeType.Direct, durable: true);

            // 普通队列
            _publishChannel.QueueDeclare(_normalQueue, normalOpt.Durable, normalOpt.Exclusive, normalOpt.AutoDelete, normalOpt.Args);
            _publishChannel.QueueBind(_normalQueue, _normalEx, _normalQueue);

            // 死信队列
            _publishChannel.QueueDeclare(_dlxQueue, dlxOpt.Durable, dlxOpt.Exclusive, dlxOpt.AutoDelete, dlxOpt.Args);
            _publishChannel.QueueBind(_dlxQueue, _dlxEx, _dlxQueue);

            Console.WriteLine("✅ TTL延迟队列+死信队列初始化完成");
        }
        #endregion

        #region 异步Confirm发布
        public async Task PublishConfirmAsync(MqMessage msg, int? customTtlMs = null)
        {
            if (_publishChannel == null || !_publishChannel.IsOpen)
                BuildConfirmPublishChannel();

            // 直接输出UTF8字节数组，无需字符串中转，无歧义
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(msg);
            var props = _publishChannel!.CreateBasicProperties();
            props.DeliveryMode = 2;

            if (customTtlMs.HasValue)
                props.Expiration = customTtlMs.Value.ToString();

            _publishChannel.BasicPublish(_normalEx, _normalQueue, basicProperties: props, body: body);
            Console.WriteLine($"[消息已发送] MsgId={msg.MsgId}, TTL={customTtlMs ?? _globalTtlMs}ms");

            await Task.CompletedTask;
        }
        #endregion

        #region 消费者：手动ACK + 幂等校验 + Nack死信
        public async Task StartConsumeAsync(Func<MqMessage, Task<bool>> businessHandler)
        {
            _consumeChannel = _conn!.CreateModel();
            _consumeChannel.BasicQos(0, 1, false); // 一次只取一条，公平消费

            var consumer = new AsyncEventingBasicConsumer(_consumeChannel);
            consumer.Received += async (_, ea) =>
            {
                var tag = ea.DeliveryTag;
                try
                {
                    // 反序列化消息
                    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                    var msg = JsonSerializer.Deserialize<MqMessage>(json)!;
                    Console.WriteLine($"\n[收到消息] MsgId={msg.MsgId} Data={msg.Data}");

                    // 幂等过期时间比TTL多30秒，防止消息未过期幂等记录提前删除
                    int expireSec = _globalTtlMs / 1000 + 30;
                    var isNew = await _idempotentSvc.TryLockMsgAsync(msg.MsgId, expireSec);
                    if (!isNew)
                    {
                        Console.WriteLine($"[幂等拦截] 重复消息{msg.MsgId}，直接ACK跳过");
                        _consumeChannel.BasicAck(tag, multiple: false);
                        return;
                    }

                    // 执行业务逻辑
                    var success = await businessHandler(msg);
                    if (success)
                    {
                        // 成功：标记幂等完成+手动ACK
                        await _idempotentSvc.MarkFinishAsync(msg.MsgId);
                        _consumeChannel.BasicAck(tag, false);
                        Console.WriteLine($"[消费成功] {msg.MsgId} ACK提交");
                    }
                    else
                    {
                        // 业务失败：释放幂等锁、Nack重回队列，超时进入死信
                        await _idempotentSvc.UnLockMsgAsync(msg.MsgId);
                        _consumeChannel.BasicNack(tag, false, requeue: true);
                        Console.WriteLine($"[消费失败] {msg.MsgId} Nack重入队列，超时进死信");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[消费异常] {ex.Message}");
                    _consumeChannel.BasicNack(tag, false, true);
                }
            };

            // autoAck=false 开启手动确认模式
            _consumeChannel.BasicConsume(_normalQueue, autoAck: false, consumer: consumer);
            Console.WriteLine("✅ 消费者启动完成，等待消息...");

            await Task.Delay(-1); // 常驻阻塞
        }
        #endregion

        #region 资源释放
        public void Dispose()
        {
            if (_disposed) return;
            _publishChannel?.Close();
            _consumeChannel?.Close();
            _conn?.Close();
            _conn?.Dispose();
            _disposed = true;
            Console.WriteLine("\nMQ连接资源已释放");
        }
        #endregion
    }
}
