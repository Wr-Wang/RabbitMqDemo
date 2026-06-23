using RabbitMqDemo.Entities;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqDemo.Interfaces
{
    /// <summary>
    /// RabbitMQ统一操作接口
    /// 能力：异步Publisher Confirm、TTL延迟+死信绑定、手动ACK消费
    /// </summary>
    public interface IRabbitMqHelper : IDisposable
    {
        /// <summary>初始化交换机、普通队列、死信队列、TTL绑定</summary>
        void InitDelayDlxQueue();

        /// <summary>异步发布消息，开启Confirm发布确认（高性能）</summary>
        Task PublishConfirmAsync(MqMessage msg, int? customTtlMs = null);

        /// <summary>启动常驻消费者，手动ACK、幂等校验、失败Nack入死信</summary>
        Task StartConsumeAsync(Func<MqMessage, Task<bool>> businessHandler);
    }
}
