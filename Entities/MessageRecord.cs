namespace RabbitMqDemo.Entities
{
    /// <summary>
    /// 消息全生命周期记录
    /// 存储在 Redis，key 格式：mq:record:{MsgId}
    /// 序列化为 JSON camelCase，通过 IMsgRecordService 访问
    /// </summary>
    public class MessageRecord
    {
        /// <summary>消息唯一 ID（与 MsgId 一致）</summary>
        public string MsgId { get; set; } = string.Empty;

        /// <summary>业务数据</summary>
        public string Data { get; set; } = string.Empty;

        /// <summary>当前状态，参见 MessageStatuses</summary>
        public string Status { get; set; } = "未消费";

        /// <summary>消息创建时间（格式：yyyy.MM.dd HH:mm:ss.fff）</summary>
        public string CreateTs { get; set; } = string.Empty;

        /// <summary>首次消费成功时间（格式：yyyy.MM.dd HH:mm:ss.fff）</summary>
        public string? ConsumeTs { get; set; }

        /// <summary>进入死信队列时间（格式：yyyy.MM.dd HH:mm:ss.fff）</summary>
        public string? DlqTs { get; set; }

        /// <summary>当前重试次数</summary>
        public int RetryCount { get; set; }
    }
}
