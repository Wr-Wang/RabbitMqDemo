using RabbitMqDemo.Entities;

namespace RabbitMqDemo.Interfaces
{
    /// <summary>
    /// 消息生命周期记录服务
    /// 将消息的发布、消费、死信、幂等各阶段状态持久化到 Redis
    /// 用于事后验证和可视化管理
    /// </summary>
    public interface IMsgRecordService
    {
        /// <summary>
        /// 消息发布成功时创建初始记录（Status=未消费）
        /// 若该 MsgId 已存在记录，则跳过不覆盖（防止重复发布覆盖已消费状态）
        /// </summary>
        Task CreateMessageRecordAsync(MqMessage msg);

        /// <summary>
        /// 更新消息状态
        /// </summary>
        /// <param name="msgId">消息唯一 ID</param>
        /// <param name="status">新状态（MessageStatuses 常量）</param>
        /// <param name="retryCount">可选：当前重试次数</param>
        Task UpdateStatusAsync(string msgId, string status, int? retryCount = null);

        /// <summary>
        /// 获取单条消息记录
        /// </summary>
        Task<MessageRecord?> GetMessageRecordAsync(string msgId);

        /// <summary>
        /// 获取所有消息记录（用于汇总展示）
        /// </summary>
        Task<List<MessageRecord>> GetAllMessageRecordsAsync();
    }
}
