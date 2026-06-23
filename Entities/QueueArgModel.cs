using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqDemo.Entities
{
    /// <summary>队列声明扩展参数（死信、TTL）</summary>
    public class QueueArgModel
    {
        public bool Durable { get; set; } = true;
        public bool Exclusive { get; set; } = false;
        public bool AutoDelete { get; set; } = false;
        public Dictionary<string, object>? Args { get; set; }
    }
}
