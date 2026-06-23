using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqDemo.Entities
{
    /// <summary>Redis存储幂等记录</summary>
    public class IdempotentRecord
    {
        /// <summary>true=已消费完成；false=处理中</summary>
        public bool Finished { get; set; }
        /// <summary>记录过期时间戳</summary>
        public long ExpireTs { get; set; }
    }
}
