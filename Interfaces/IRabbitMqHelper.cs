using RabbitMqDemo.Entities;

namespace RabbitMqDemo.Interfaces
{
    /// <summary>
    /// RabbitMQ 统一操作接口
    ///
    /// 核心能力：
    ///   1. TTL 延迟 + 死信队列绑定
    ///   2. Publisher Confirm 异步发布
    ///   3. 手动 ACK 消费 + Redis 幂等 + 失败 Nack 入死信
    ///   4. 通用 Ex/Queue 声明 + 发布 + 消费（支撑多场景验证）
    ///
    /// 使用方式：
    ///   var mqHelper = serviceProvider.GetRequiredService&lt;IRabbitMqHelper&gt;();
    ///   mqHelper.InitDelayDlxQueue();
    ///   await mqHelper.PublishConfirmAsync(msg);
    ///   await mqHelper.StartConsumeAsync(handler, ct);
    /// </summary>
    public interface IRabbitMqHelper : IDisposable
    {
        /// <summary>
        /// 初始化交换机、普通队列、死信队列、TTL 绑定
        /// 幂等方法，可重复调用
        ///
        /// 创建以下基础设施：
        ///   - biz.normal.ex（Direct）：普通消息交换机
        ///   - biz.normal.queue：普通队列，绑定 x-dead-letter-exchange=biz.dlx.ex
        ///   - biz.dlx.ex（Direct）：死信交换机
        ///   - biz.dlx.queue：死信队列
        ///   队列的 x-message-ttl 从 appsettings.json MqBusinessConfig:MessageTtlMs 读取
        ///
        /// 消费者处理失败时 Nack requeue=true，消息在队列中持续重试直到 TTL 超时，
        /// 超时后自动转发到死信交换机 → 死信队列，实现延迟重试 + 死信兜底。
        /// </summary>
        void InitDelayDlxQueue();

        /// <summary>
        /// 异步发布消息（开启 Confirm 确认）
        /// 等待 Broker 确认后返回，保证消息可靠到达
        ///
        /// 发布流程：
        ///   1. 序列化 MqMessage 为 UTF-8 JSON 字节
        ///   2. 设置 DeliveryMode=2（持久化）
        ///   3. BasicPublish 到 biz.normal.ex → biz.normal.queue
        ///   4. WaitForConfirmsOrDie（5s 超时）等待 Broker 确认
        ///   5. 写入 Redis 消息记录（mq:record:{MsgId}，初始状态="未消费"）
        ///
        /// 幂等说明：Producer 侧不检查重复，幂等由 Consumer 侧通过 Redis SETNX + Lua 完成。
        /// 如需模拟重复消息，重复调用此方法传入相同 MsgId 即可。
        /// </summary>
        /// <param name="msg">消息体，MsgId 必填且建议全局唯一（重复投递相同 MsgId 可模拟幂等场景）</param>
        /// <param name="customTtlMs">可选：覆盖队列级别的消息 TTL（毫秒）。
        ///     队列默认 TTL 从 appsettings.json 读取。传入此参数将设置消息级 Expiration 属性，
        ///     覆盖队列的 x-message-ttl。用于延迟队列等需要单条消息不同超时的场景。
        ///     为 null 时使用队列级 TTL。</param>
        Task PublishConfirmAsync(MqMessage msg, int? customTtlMs = null);

        /// <summary>
        /// 启动常驻消费者
        /// 特性：手动 ACK + Redis 幂等 + 失败 Nack 重试 + 异常入死信
        ///
        /// 消费流程：
        ///   1. 创建独立通道，设置 BasicQos(0,1,false) 公平分发
        ///   2. 注册 EventingBasicConsumer（同步回调，在 IO 线程执行）
        ///   3. 收到消息 → 反序列化 → 幂等校验（Redis SETNX + Lua）
        ///      a. 已消费完成（Finished=true）→ 幂等跳过，ACK
        ///      b. 首次或崩溃恢复 → 执行业务委托
        ///   4. 业务成功 → MarkFinish（Finished=true）+ ACK
        ///   5. 业务失败 → UnLockMsg + Nack requeue（超时后入死信）
        ///   6. 异常兜底 → Nack 不重入（requeue=false），防止无限循环
        ///
        /// 注意：该方法通过 Task.Delay(-1, ct) 阻塞，直到 cancellationToken 被取消。
        /// </summary>
        /// <param name="businessHandler">业务处理委托。
        ///     参数 msg：反序列化后的消息体。
        ///     返回值 true：处理成功，标记幂等完成并 ACK 确认。
        ///     返回值 false：处理失败，释放幂等锁并 Nack requeue（队列内持续重试直到 TTL 超时进死信）。
        ///     委托内应避免长时间同步阻塞（建议 &lt;= 200ms），否则可能触发 RabbitMQ 心跳超时。</param>
        /// <param name="cancellationToken">取消令牌。
        ///     传入 CancellationTokenSource.CreateLinkedTokenSource 可组合多个取消源。
        ///     取消时消费者优雅退出：处理中的消息继续完成，不再接收新消息。
        ///     常用模式：CancelAfter(TimeSpan.FromSeconds(N)) 实现超时自动退出。</param>
        Task StartConsumeAsync(Func<MqMessage, Task<bool>> businessHandler, CancellationToken cancellationToken = default);

        /// <summary>
        /// 启动死信队列消费者
        /// 自动消费已进入死信队列的消息，支持自定义业务处理
        /// 默认行为：记录死信消息并确认（从队列移除）
        ///
        /// 死信来源：
        ///   - 主消费者 Nack requeue → 重试直到 x-message-ttl(8s) 超时 → 转发到 DLQ
        ///   - 消息过期（队列设置了 x-message-ttl）
        ///
        /// 消费流程：
        ///   1. 创建独立通道，BasicQos(0,1,false)
        ///   2. 收到死信消息 → 反序列化 → 显示 x-death 头信息（死信原因、原队列、重试次数）
        ///   3. 更新 Redis 记录状态为 "已进死信"
        ///   4. 调用外部业务委托（若有）
        ///   5. 成功 Ack，失败 Nack 丢弃（不进二次死信）
        ///
        /// 注意：该方法通过 Task.Delay(-1, ct) 阻塞，直到 cancellationToken 被取消。
        /// </summary>
        /// <param name="businessHandler">可选死信处理委托。
        ///     参数 msg：反序列化后的死信消息。
        ///     返回值 true：确认处理完成，从 DLQ 移除。
        ///     返回值 false：拒绝处理，丢弃消息（不进二次死信）。
        ///     传入 null 时仅记录日志并 Ack（默认兜底行为）。</param>
        /// <param name="cancellationToken">取消令牌。
        ///     建议与主消费者共享同一个 CancellationTokenSource，确保同时退出。</param>
        Task StartDlxConsumeAsync(Func<MqMessage, Task<bool>>? businessHandler = null, CancellationToken cancellationToken = default);

        // ===== 以下为 6 大场景验证通用方法 =====

        /// <summary>
        /// 声明交换机 + 队列 + 绑定
        /// 支持 Direct / Fanout / Topic 三种类型
        ///
        /// 方法内部依次执行：
        ///   1. ExchangeDeclare（持久化 durable=true）
        ///   2. QueueDeclare（持久化 durable=true, autoDelete=false）
        ///   3. QueueBind（按指定 routingKey 绑定）
        ///
        /// 幂等性：RabbitMQ 中重复声明同名交换机/队列不会报错，
        /// 参数不匹配时会触发警告但不会抛异常（需注意参数一致性）。
        ///
        /// 使用场景：
        ///   - 异步处理：Direct 绑定，单队列单消费者
        ///   - 服务解耦：Fanout 绑定，同一交换机绑定多个队列
        ///   - 削峰填谷：Direct 绑定，多消费者竞争同一队列
        ///   - 延迟队列：TTL + DLX 参数，source→target 转发
        ///   - 分布式事务：Direct 绑定，多队列各司其职
        ///   - 消息广播：Fanout 绑定，一对多通知
        /// </summary>
        /// <param name="exchange">交换机名称。
        ///     命名建议：{业务域}.{场景}.ex，如 demo.async.ex。
        ///     重复调用时如果交换机已存在且参数一致，不会报错。</param>
        /// <param name="exchangeType">交换机类型。
        ///     可选值："direct"、"fanout"、"topic"。
        ///     对应 RabbitMQ.Client.ExchangeType.Direct / Fanout / Topic。
        ///     - direct：按 routingKey 精确匹配（默认）。
        ///     - fanout：广播到所有绑定队列，忽略 routingKey。
        ///     - topic：按 routingKey 模式匹配（* 匹配一个词，# 匹配多个词）。</param>
        /// <param name="queue">队列名称。
        ///     命名建议：{业务域}.{场景}.queue，如 demo.async.queue。
        ///     持久化 durable=true、不自动删除 autoDelete=false。
        ///     对于 Fanout 交换机，路由键传空字符串 ""，队列名区分不同消费者。</param>
        /// <param name="routingKey">路由键。
        ///     Direct 交换机：通常与队列名相同。
        ///     Fanout 交换机：被忽略，传空字符串 "" 即可。
        ///     Topic 交换机：模式字符串，如 "order.*"、"order.#"。</param>
        /// <param name="arguments">可选：队列参数（Dictionary<string, object>）。
        ///     常用参数：
        ///     - x-message-ttl：消息过期时间（毫秒），超时后根据 DLX 配置转发或丢弃
        ///     - x-dead-letter-exchange：死信交换机名，过期/Nack 消息转发目标
        ///     - x-dead-letter-routing-key：死信路由键，转发到死信队列时使用
        ///     - x-max-priority：队列优先级（0-255），需配合消息的 priority 属性
        ///     - x-max-length：队列最大消息数，超限时最早消息被丢弃或转发到 DLX
        ///     示例：
        ///     new Dictionary&lt;string, object&gt;
        ///     {
        ///         ["x-message-ttl"] = 5000,
        ///         ["x-dead-letter-exchange"] = "demo.delay.ex"
        ///     }</param>
        void DeclareExQueue(string exchange, string exchangeType, string queue,
            string routingKey, IDictionary<string, object>? arguments = null);

        /// <summary>
        /// 发布消息到指定交换机
        /// 支持设置消息级过期时间
        ///
        /// 与 PublishConfirmAsync 的区别：
        ///   - 此方法可指定目标 exchange/routingKey，不限于 biz.normal.ex
        ///   - 同样开启 Publisher Confirm（WaitForConfirmsOrDie 5s 超时）
        ///   - 同样记录 Redis 消息记录（首次发布则创建，重复发布跳过）
        ///
        /// 发布流程：
        ///   1. 序列化 MqMessage 为 UTF-8 JSON
        ///   2. 设置 DeliveryMode=2 持久化
        ///   3. 可选设置 Expiration 消息级 TTL
        ///   4. BasicPublish 到指定 exchange
        ///   5. WaitForConfirmsOrDie 等待 Broker 确认
        ///   6. 写入 Redis 消息记录
        ///
        /// 适用场景：
        ///   - 向 Demo 验证场景的特定交换机/队列发送消息
        ///   - 需要设置消息级 TTL 的场景
        /// </summary>
        /// <param name="exchange">目标交换机名称。
        ///     必须已存在（通过 DeclareExQueue 或 RabbitMQ 管理端提前创建）。
        ///     如 "demo.async.ex"、"demo.delay.ex"。</param>
        /// <param name="routingKey">路由键。
        ///     Direct 交换机：目标队列名。
        ///     Fanout 交换机：忽略，传 ""（空字符串）即可。
        ///     Topic 交换机：模式匹配字符串。</param>
        /// <param name="msg">消息体。
        ///     MsgId 用于 Redis 持久化记录和幂等判断。
        ///     Data 为业务内容。
        ///     CreateTs 自动设置为当前时间。
        ///     首次发布的 MsgId 会创建 Redis 记录（状态="未消费"），
        ///     重复 MsgId 的发布不会覆盖已有记录。</param>
        /// <param name="expirationMs">可选：消息级过期时间（毫秒）。
        ///     设置后覆盖队列级别的 x-message-ttl。
        ///     仅对当前消息生效，不影响队列中其他消息。
        ///     对于延迟队列场景：设置过期时间后，消息在源队列等待指定毫秒数，
        ///     超时后根据队列的 x-dead-letter-exchange 配置转发到目标队列。</param>
        Task PublishToExchangeAsync(string exchange, string routingKey, MqMessage msg,
            int? expirationMs = null);

        /// <summary>
        /// 启动指定队列的通用消费者（手动 ACK）
        /// 内部创建独立通道，不影响其他消费者
        ///
        /// 消费模式：
        ///   - autoAck=false（手动确认）
        ///   - BasicQos(0, 1, false) 公平分发，一次只取一条
        ///   - 使用 EventingBasicConsumer 同步回调（IO 线程）
        ///   - 委托返回 true → BasicAck；返回 false → BasicNack 丢弃
        ///   - 异常兜底 → BasicNack 不重入
        ///
        /// 通道管理：
        ///   - 每次调用创建独立的 IModel 通道
        ///   - 受 _extraChannels 列表管理，RabbitMqHelper.Dispose() 时统一清理
        ///   - 多个消费者共享同一个连接 IConnection（单例）
        ///
        /// 适用场景：
        ///   - 多个消费者并发消费同一队列（削峰填谷）
        ///   - 多个消费者各自消费不同的队列（服务解耦、分布式事务）
        ///   - 异步后台处理（长时间任务）
        ///
        /// 注意：
        ///   1. 该方法通过 Task.Delay(-1, ct) 阻塞，直到 cancellationToken 被取消
        ///   2. 消费者回调在 IO 线程执行，避免长时间同步阻塞（超过心跳 60s 会被服务端断开）
        ///   3. 如需异步回调（async/await），使用 AsyncEventingBasicConsumer 替代
        ///   4. 此消费者不包含 Redis 幂等逻辑（由主消费者 StartConsumeAsync 提供）
        /// </summary>
        /// <param name="queue">要消费的队列名称。
        ///     队列必须已存在（通过 DeclareExQueue 或管理端创建）。
        ///     示例："demo.async.queue"、"demo.spike.queue"。</param>
        /// <param name="handler">消息处理委托。
        ///     参数 msg：反序列化后的消息。
        ///     返回值 true：处理成功，BasicAck 确认，消息从队列移除。
        ///     返回值 false：处理失败，BasicNack(requeue=false) 丢弃消息。
        ///     如需 requeue 重试，请在委托内部返回 false。
        ///     委托内可使用 Thread.Sleep 模拟耗时，但建议 &lt;= 心跳超时（默认 60s）。
        ///     对于需等待 I/O 的操作，使用 async 委托并在内部 await 异步方法。</param>
        /// <param name="cancellationToken">取消令牌。
        ///     建议与主消费者共享同一个 CancellationTokenSource（同生共死）。
        ///     取消时消费者优雅退出：当前消息继续处理，完成后停止接收。</param>
        Task StartQueueConsumerAsync(string queue, Func<MqMessage, Task<bool>> handler,
            CancellationToken cancellationToken = default);
    }
}
