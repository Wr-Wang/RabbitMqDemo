namespace RabbitMqDemo.Entities
{
    /// <summary>
    /// 消息生命周期状态常量
    /// 存储于消息记录（mq:record:*）的 status 字段
    /// </summary>
    public static class MessageStatuses
    {
        /// <summary>消息已发布到队列，尚未被消费</summary>
        public const string Published = "未消费";

        /// <summary>消费成功，业务处理完成</summary>
        public const string Consumed = "已消费";

        /// <summary>消息进入死信队列（Nack 重试超期）</summary>
        public const string DeadLettered = "已进死信";

        /// <summary>重复消息被幂等拦截，跳过处理</summary>
        public const string IdempotentSkipped = "幂等跳过";

        /// <summary>业务处理失败（将 Nack 重试）</summary>
        public const string ConsumerFailed = "消费失败";
    }
}
