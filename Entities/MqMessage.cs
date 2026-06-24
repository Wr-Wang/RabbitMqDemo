namespace RabbitMqDemo.Entities
{
    /// <summary>
    /// 统一 MQ 消息载体
    ///
    /// MsgId 是幂等判断的核心依据，必须全局唯一
    /// 建议格式：{业务前缀}_{GUID}，如 "order_550e8400e29b41d4"
    /// </summary>
    public class MqMessage
    {
        /// <summary>唯一消息 ID，幂等判断依据（必填，重复会跳过处理）</summary>
        public string MsgId { get; set; } = string.Empty;

        /// <summary>业务数据（JSON 字符串或纯文本）</summary>
        public string Data { get; set; } = string.Empty;

        /// <summary>消息生成时间戳（Unix 毫秒，UTC）</summary>
        public long CreateTs { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
