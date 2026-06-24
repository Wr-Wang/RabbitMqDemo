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
    /// </summary>
    public interface IRabbitMqHelper : IDisposable
    {
        /// <summary>
        /// 初始化交换机、普通队列、死信队列、TTL 绑定
        /// 幂等方法，可重复调用
        /// </summary>
        void InitDelayDlxQueue();

        /// <summary>
        /// 异步发布消息（开启 Confirm 确认）
        /// 等待 Broker 确认后返回，保证消息可靠到达
        /// </summary>
        /// <param name="msg">消息体（MsgId 唯一标识，用于幂等）</param>
        /// <param name="customTtlMs">可选：覆盖队列级别的消息 TTL（毫秒）</param>
        Task PublishConfirmAsync(MqMessage msg, int? customTtlMs = null);

        /// <summary>
        /// 启动常驻消费者
        /// 特性：手动 ACK + Redis 幂等 + 失败 Nack 重试 + 异常入死信
        /// </summary>
        /// <param name="businessHandler">业务处理委托（true=成功ACK, false=失败Nack重试）</param>
        /// <param name="cancellationToken">取消令牌，用于优雅关闭</param>
        Task StartConsumeAsync(Func<MqMessage, Task<bool>> businessHandler, CancellationToken cancellationToken = default);

        /// <summary>
        /// 启动死信队列消费者
        /// 自动消费已进入死信队列的消息，支持自定义业务处理
        /// 默认行为：记录死信消息并确认（从队列移除）
        /// </summary>
        /// <param name="businessHandler">可选死信处理委托（true=确认删除, false=拒绝丢弃）</param>
        /// <param name="cancellationToken">取消令牌，用于优雅关闭</param>
        Task StartDlxConsumeAsync(Func<MqMessage, Task<bool>>? businessHandler = null, CancellationToken cancellationToken = default);
    }
}
