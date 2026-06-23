using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqDemo.Interfaces
{
    /// <summary>Redis消息幂等服务</summary>
    public interface IMsgIdempotentService
    {
        /// <summary>原子加锁校验，true=全新消息可消费；false=重复消息</summary>
        Task<bool> TryLockMsgAsync(string msgId, int expireSecond);

        /// <summary>消费成功标记完成</summary>
        Task MarkFinishAsync(string msgId);

        /// <summary>消费失败释放锁，允许重试</summary>
        Task UnLockMsgAsync(string msgId);
    }
}
