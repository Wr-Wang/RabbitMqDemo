namespace RabbitMqDemo.Entities
{
    /// <summary>
    /// Redis 幂等记录
    ///
    /// 序列化为 JSON 存储在 Redis，camelCase 命名：
    ///   {"finished":true}
    ///
    /// 注意：Lua 脚本中用 record['finished']（小写 f）访问
    /// </summary>
    public class IdempotentRecord
    {
        /// <summary>
        /// 消费完成标志
        /// true  = 已消费完成（重复消息直接跳过）
        /// false = 处理中（首次 或 崩溃后重新处理）
        /// </summary>
        public bool Finished { get; set; }
    }
}
