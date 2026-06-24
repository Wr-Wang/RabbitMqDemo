namespace RabbitMqDemo.Interfaces
{
    /// <summary>
    /// Redis 消息幂等服务接口
    ///
    /// 核心流程：
    ///   TryLockMsgAsync（加锁/检查）
    ///      ↓ success → MarkFinishAsync（标记完成）
    ///      ↓ failure → UnLockMsgAsync（释放锁，允许重试）
    /// </summary>
    public interface IMsgIdempotentService
    {
        /// <summary>
        /// 原子加锁校验
        /// </summary>
        /// <param name="msgId">消息唯一 ID</param>
        /// <param name="expireSecond">锁过期时间（秒）</param>
        /// <returns>true=首次加锁成功（可消费）, false=已消费完成（跳过）</returns>
        Task<bool> TryLockMsgAsync(string msgId, int expireSecond);

        /// <summary>
        /// 消费成功标记完成
        /// 将 Finished 设为 true，后续重复消息直接跳过
        /// </summary>
        Task MarkFinishAsync(string msgId);

        /// <summary>
        /// 消费失败释放锁
        /// 删除 Redis 记录，允许消息重新入队后再次消费
        /// </summary>
        Task UnLockMsgAsync(string msgId);
    }
}
