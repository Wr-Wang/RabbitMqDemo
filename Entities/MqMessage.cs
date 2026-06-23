using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqDemo.Entities
{
    /// <summary>
    /// 统一MQ消息载体（幂等核心MsgId）
    /// </summary>
    public class MqMessage
    {
        /// <summary>唯一消息ID，幂等判断依据</summary>
        public string MsgId { get; set; } = string.Empty;
        /// <summary>业务数据</summary>
        public string Data { get; set; } = string.Empty;
        /// <summary>消息生成时间戳</summary>
        public long CreateTs { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
