# RabbitMQ Demo — 消息中间件应用场景验证项目

基于 RabbitMQ + Redis 的消息中间件应用场景验证项目，覆盖发布订阅、幂等消费、死信队列、削峰填谷、延迟队列、分布式事务等 8 大典型场景。

---

## 目录

- [项目结构](#项目结构)
- [配置说明](#配置说明)
- [核心能力](#核心能力)
- [性能优化](#性能优化)
- [函数级逻辑详解](#函数级逻辑详解)
- [6 大应用场景](#6-大应用场景)
- [运行指南](#运行指南)
- [输出解读](#输出解读)
- [依赖组件](#依赖组件)

---

## 项目结构

```
RabbitMqDemo/
├── Program.cs                      # 入口：9 个 Block 分块验证所有场景
├── appsettings.json                # 配置：RabbitMQ + Redis + 业务队列参数
├── RabbitMqDemo.csproj             # 项目文件（.NET 10）
│
├── Entities/                       # 数据实体
│   ├── MqMessage.cs                #   MQ 消息载体（MsgId / Data / CreateTs）
│   ├── MessageRecord.cs            #   Redis 消息全生命周期记录
│   ├── MessageStatuses.cs          #   消息状态常量（5 种状态）
│   └── IdempotentRecord.cs         #   幂等锁记录（Finished 布尔值）
│
├── Interfaces/                     # 服务接口
│   ├── IRabbitMqHelper.cs          #   RabbitMQ 统一操作接口（7 个方法）
│   ├── IMsgIdempotentService.cs    #   消息幂等服务接口
│   └── IMsgRecordService.cs        #   消息生命周期记录服务接口
│
├── Services/                       # 服务实现
│   ├── RabbitMqHelper.cs           #   RabbitMQ 助手（连接/发布/消费/声明）
│   ├── RedisIdempotentService.cs   #   Redis 幂等锁（SETNX + Lua 原子脚本）
│   └── RedisMsgRecordService.cs    #   Redis 消息记录（JSON 持久化）
│
└── Extensions/                     # DI 注册扩展
    ├── MqServiceCollectionExt.cs   #   IRabbitMqHelper 注册
    └── RedisServiceCollectionExt.cs #   Redis 相关服务注册
```

---

## 配置说明

### appsettings.json

```json
{
  "RabbitConfig": {
    "Host": "127.0.0.1",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "VirtualHost": "/"
  },
  "RedisConfig": {
    "ConnStr": "127.0.0.1:6379,password=123456,defaultDatabase=0"
  },
  "MqBusinessConfig": {
    "NormalExchange": "biz.normal.ex",
    "NormalQueue": "biz.normal.queue",
    "DlxExchange": "biz.dlx.ex",
    "DlxQueue": "biz.dlx.queue",
    "MessageTtlMs": 8000
  }
}
```

| 配置节 | 参数 | 说明 |
|--------|------|------|
| RabbitConfig | Host / Port / UserName / Password | RabbitMQ 连接（默认 localhost:5672 guest/guest） |
| RabbitConfig | VirtualHost | 虚拟主机，默认 "/" |
| RedisConfig | ConnStr | Redis 连接字符串，含密码和数据库索引 |
| MqBusinessConfig | NormalExchange / NormalQueue | 主业务 Direct 交换机/队列 |
| MqBusinessConfig | DlxExchange / DlxQueue | 死信交换机/队列 |
| MqBusinessConfig | MessageTtlMs | 消息 TTL（毫秒），超时进死信 |

---

## 核心能力

### 1. 消息发布（Publisher Confirm）

```csharp
await mqHelper.PublishConfirmAsync(msg);
```

- 开启 Confirm 模式，`WaitForConfirmsOrDie(5s)` 等待 Broker 确认
- `DeliveryMode=2` 持久化，重启不丢
- 发布成功后自动写入 Redis 消息记录

### 2. 消息消费 + Redis 幂等去重

```csharp
await mqHelper.StartConsumeAsync(async msg => { ... }, ct);
```

**消费流程（6 步）：**

```
收到消息 → 反序列化
    ↓
Redis 幂等校验（Lua 原子脚本）
    ├─ Finished=true → 幂等跳过 → ACK
    └─ 首次/崩溃恢复 → 执行业务处理
         ├─ 成功 → MarkFinish(Finished=true) + ACK
         └─ 失败 → UnLockMsg + Nack requeue → 超时进死信
```

**幂等锁实现（Redis Lua 脚本）：**

```lua
-- key = mq:idempotent:{MsgId}
-- val = {"finished":true/false}
GET key → 存在且 finished=true → return 0（跳过）
否则 → SETEX key ttl {"finished":false} → return 1（可消费）
```

> 幂等锁 TTL = 队列 TTL + 30s，确保消息存续期内锁不提前过期。

### 3. 死信队列（DLX/DLQ）

- Nack requeue=true 的消息在队列中持续重试
- 超过 `x-message-ttl`（默认 8s）后自动转发到 DLX → DLQ
- 死信消费者接收后更新 Redis 记录状态为 **"已进死信"**
- 死信消息携带 `x-death` 头信息（死信原因、原队列、重试次数）

### 4. 消息全生命周期持久化（Redis）

| Redis Key 前缀 | 用途 | TTL |
|----------------|------|-----|
| `mq:idempotent:{MsgId}` | 幂等锁（Lua 原子读写） | 队列 TTL + 30s |
| `mq:record:{MsgId}` | 全生命周期消息记录 | 无（永久保存） |

消息记录包含 5 种状态流转：

```
发布 → status="未消费"
  ↓
消费成功 → status="已消费" + consumeTs 记录
消费失败 → status="消费失败" + retryCount++
幂等跳过 → status="幂等跳过"
进入死信 → status="已进死信" + dlqTs 记录
```

### 5. 通用 Ex/Queue 声明 + 发布 + 消费

```csharp
// 声明
mqHelper.DeclareExQueue("demo.async.ex", "direct", "demo.async.queue", "demo.async.queue");

// 发布
await mqHelper.PublishToExchangeAsync("demo.async.ex", "demo.async.queue", msg);

// 消费（通用消费者，手动 ACK）
await mqHelper.StartQueueConsumerAsync("demo.async.queue", async msg => { ... }, ct);
```

支持 Direct / Fanout / Topic 三种交换机类型。队列参数支持 `x-message-ttl`、`x-dead-letter-exchange`、`x-max-priority` 等。

---
## 性能优化

### 已实施的优化

#### 1. 零拷贝反序列化 — 减少内存分配

**改动位置：** `RabbitMqHelper.cs` — 3 个消费者的反序列化入口

```diff
- var json = Encoding.UTF8.GetString(ea.Body.ToArray());
+ var json = Encoding.UTF8.GetString(ea.Body.Span);
```

`ea.Body` 类型为 `ReadOnlyMemory<byte>`。`.ToArray()` 每次分配新数组拷贝数据；`.Span` 直接引用底层内存，零拷贝。

#### 2. 保留 EventingBasicConsumer（同步消费回调）

**背景：** 曾尝试将 `EventingBasicConsumer` 替换为 `AsyncEventingBasicConsumer` 以消除 IO 线程阻塞，但通过 IL 分析和反射验证发现 **此路不通**，原因如下：

```
IModel.BasicConsume 签名：
  string BasicConsume(string queue, bool autoAck, IBasicConsumer consumer)
  → 只接受 IBasicConsumer，无 IAsyncBasicConsumer 重载

AsyncEventingBasicConsumer 实现了两个接口：
  - IBasicConsumer  ✓（继承自 AsyncDefaultBasicConsumer）
  - IAsyncBasicConsumer  ✓

框架通过 IBasicConsumer 路径调用时：
  AsyncDefaultBasicConsumer.IBasicConsumer.HandleBasicDeliver
  → IL 字节码：ldstr → call NotSupportedException → throw
  → Received 事件永不触发
  → 消息被静默丢弃
```

**结论：** RabbitMQ.Client 6.8.1 中 `IModel.BasicConsume` 不支持异步消费者。
必须使用 `EventingBasicConsumer`，异步 Redis 调用通过 `.GetAwaiter().GetResult()` 同步等待。
IO 线程短暂阻塞（Redis 响应通常 &lt; 5ms）对吞吐量影响有限，远小于消息静默丢失的代价。

### 建议进一步优化的方向

| 优化项 | 建议方式 | 适用场景 | 备注 |
|--------|---------|---------|------|
| **批量 Confirm** | 多次 `BasicPublish` 后一次 `WaitForConfirmsOrDie` | 削峰填谷批量发送 | 减少 RTT，但增加单条确认延迟 |
| **提升 Prefetch** | `BasicQos(0, 10, false)` 预取多条 | 削峰填谷消费端 | 以内存换取吞吐量，配合多消费者 |
| **合并 Redis 操作** | 幂等校验 + 状态更新合并为一次 Lua 脚本 | 有性能压测要求的场景 | 减少网络往返，增加脚本复杂度 |
| **消息优先级** | 队列设置 `x-max-priority`，消息设 `priority` | 关键业务优先消费 | 需 Broker 版本 3.5+ |
| **升级 IChannel** | 使用 `IChannel` 和 `BasicConsumeAsync`（未来版本） | 彻底消除 IO 线程阻塞 | 需 RabbitMQ.Client 7+ 支持 |

---

## 函数级逻辑详解

### RabbitMqHelper.cs — 7 个公开方法

---

#### `InitDelayDlxQueue()` — 初始化死信队列基础设施

**功能：** 创建主业务交换机和队列、死信交换机和队列，普通队列绑定 TTL + DLX 参数。

**内部步骤：**

```
① 读取 MqBusinessConfig 配置节 → 获取 NormalExchange/Queue、DlxExchange/Queue、MessageTtlMs
② 构建 normalArgs 字典：
   └─ x-dead-letter-exchange → biz.dlx.ex
   └─ x-dead-letter-routing-key → biz.dlx.queue
   └─ x-message-ttl → 8000（ms）
③ ExchangeDeclare(biz.normal.ex, Direct, durable:true)
④ ExchangeDeclare(biz.dlx.ex, Direct, durable:true)
⑤ QueueDeclare(biz.normal.queue, durable, arguments:normalArgs)
   └─ 队列携带 TTL+DLX 参数，消息超时自动转发到 DLX
⑥ QueueBind(biz.normal.queue → biz.normal.ex, routingKey=biz.normal.queue)
⑦ QueueDeclare(biz.dlx.queue, durable)
⑧ QueueBind(biz.dlx.queue → biz.dlx.ex, routingKey=biz.dlx.queue)
```

**设计要点：**
- 幂等方法：RabbitMQ 中重复声明同名 Ex/Queue 不报错
- 队列持久化（durable:true），RabbitMQ 重启后队列不丢失
- TTL 从配置读取，方便调整

---

#### `PublishConfirmAsync(MqMessage, int?)` — 消息发布（Confirm 模式）

**功能：** 发布消息到主业务交换机，等待 Broker 确认，写入 Redis 记录。

**内部步骤：**

```
① 检查 _publishChannel 是否 Open → 否 → 调用 BuildConfirmPublishChannel() 重建
② JsonSerializer.SerializeToUtf8Bytes(msg) → byte[] body
③ channel.CreateBasicProperties() → props
   └─ props.DeliveryMode = 2（持久化，重启不丢失）
   └─ 如果 customTtlMs 有值 → props.Expiration = customTtlMs.ToString()
      └─ 注意：Expiration 是字符串，单位毫秒
④ channel.BasicPublish(biz.normal.ex, biz.normal.queue, props, body)
⑤ await Task.Run(() => WaitForConfirmsOrDie(TimeSpan.FromSeconds(5)))
   └─ Task.Run：将同步阻塞调用移到线程池，避免阻塞 async 调用方
   └─ WaitForConfirmsOrDie：同步等待 Broker 的 basic.ack
   └─ 5s 超时未确认 → 抛出 IOException（Broker 可能宕机）
⑥ try { msgRecordSvc.CreateMessageRecordAsync(msg) }
   └─ 写入 Redis，状态="未消费"
   └─ 异常 catch 不抛：记录失败不影响消息本身
```

**关键设计：**
- 通道自动重建：网络闪断后 _publishChannel 被关闭，下次发布前自动新建
- Redis 记录是辅助功能，不影响核心消息投递

---

#### `StartConsumeAsync(Func, CancellationToken)` — 主消费者（幂等 + 死信）

**功能：** 启动常驻消费者，接收主业务队列消息，执行 Redis 幂等校验 + 业务处理 + 死信转发。

**内部步骤：**

```
① _conn.CreateModel() → _consumeChannel（独立通道）
② _consumeChannel.BasicQos(0, 1, false)
   └─ prefetchSize=0（不限制）
   └─ prefetchCount=1（每次只取一条，公平分发）
   └─ global=false（仅当前消费者生效）
③ new EventingBasicConsumer(_consumeChannel)
   └─ 注册 consumer.Received 同步回调（IO 线程执行）
   └─ 异步 Redis 调用通过 .GetAwaiter().GetResult() 同步等待
   └─ 注：AsyncEventingBasicConsumer 因 IBasicConsumer 接口限制无法使用

回调内逻辑（收到消息时触发）：

第 1 步 - 反序列化（零拷贝）：
   ① Encoding.UTF8.GetString(ea.Body.Span) → json
      └─ .Span 直接引用 ReadOnlyMemory 底层内存，无需 ToArray() 拷贝
   ② JsonSerializer.Deserialize<MqMessage>(json) → msg
   ③ msg == null → BasicNack(tag, false, false) → return

第 2 步 - 幂等校验：
   ① expireSec = globalTtlMs / 1000 + 30（TTL + 30s 冗余）
   ② idempotentSvc.TryLockMsgAsync(msg.MsgId, expireSec)
      └─ Lua 脚本原子执行（详见 RedisIdempotentService）
   ③ isNew == false → Redis 中已存在 Finished=true 记录
      ├─ msgRecordSvc.UpdateStatusAsync(幂等跳过)
      └─ BasicAck → return（跳过处理）

第 3 步 - 业务处理：
   ① businessHandler(msg) → 调用外部委托
   ② 返回值：
      true（成功）→
        ├─ idempotentSvc.MarkFinishAsync(Finished=true)
        ├─ msgRecordSvc.UpdateStatusAsync(已消费)
        └─ BasicAck
      false（失败）→
        ├─ idempotentSvc.UnLockMsgAsync(删除锁)
        ├─ msgRecordSvc.UpdateStatusAsync(消费失败, retryCount+1)
        └─ BasicNack(tag, false, requeue:true)
           └─ 消息回到队列头部，重新投递 → 继续尝试直到 TTL 超时进死信

第 4 步 - 异常兜底：
   ① catch (Exception ex) → 日志输出异常类型和信息
   ② RedisConnectionException → 输出 Redis 连接串提示
   ③ BasicNack(tag, false, false) → 不重入，防止无限循环
      └─ requeue=false：消息被丢弃或转发到 DLX（取决于队列 DLX 配置）

④ channel.BasicConsume(biz.normal.queue, autoAck:false, consumer)
   └─ 注册消费者到队列
⑤ await Task.Delay(-1, cancellationToken) 阻塞直到被取消
   └─ 被取消时 catch OperationCanceledException，打印退出日志
```

**关键设计：**
- 单通道单消费者：BasicQos(0,1,false) 确保一次一条，不会有多条消息堆积在消费者内存
- 同步回调：EventingBasicConsumer + .GetAwaiter().GetResult()，Redis 调用期间短阻塞
- 失败重试链：Nack requeue → 队列重新投递 → 重复尝试 → TTL 超时 → DLX → DLQ

---

#### `DeclareExQueue(string, string, string, string, IDictionary)` — 通用 Ex/Queue 声明

**功能：** 声明任意交换机 + 队列 + 绑定，支持三种类型和队列参数。

**内部步骤：**

```
① ExchangeDeclare(exchange, type, durable:true)
   └─ 持久化交换机，RabbitMQ 重启后保留
② QueueDeclare(queue, durable:true, exclusive:false, autoDelete:false, arguments)
   └─ 持久化 / 不排他 / 不自动删除
   └─ arguments 可选：x-message-ttl、x-dead-letter-exchange 等
③ QueueBind(queue, exchange, routingKey)
   └─ 按 routingKey 绑定到交换机
   └─ Fanout 类型时 routingKey 被忽略
```

**参数组合示例：**

| exchangeType | routingKey | 效果 |
|-------------|-----------|------|
| "direct" | 队列名 | 精确匹配，消息只到该队列 |
| "fanout" | "" | 广播到所有绑定队列 |
| "topic" | "order.*" | 模式匹配路由 |

---

#### `PublishToExchangeAsync(string, string, MqMessage, int?)` — 通用消息发布

**功能：** 发布消息到任意指定交换机，支持消息级过期时间。

**内部步骤：**

```
① 通道可用性检查 → 不可用时自动重建
② JsonSerializer.SerializeToUtf8Bytes(msg) → body
③ props.DeliveryMode = 2
④ expirationMs 有值 → props.Expiration = expirationMs.ToString()
   └─ 消息级 TTL，覆盖队列的 x-message-ttl
⑤ channel.BasicPublish(exchange, routingKey, props, body)
⑥ await Task.Run(() => WaitForConfirmsOrDie(5s))
⑦ try { msgRecordSvc.CreateMessageRecordAsync(msg) }
   └─ 首次发布创建，重复 MsgId 跳过
```

**与 PublishConfirmAsync 的核心区别：**

| 方法 | 目标 Exchange | 场景 |
|------|-------------|------|
| PublishConfirmAsync | biz.normal.ex（固定） | 主业务消息发送 |
| PublishToExchangeAsync | 参数指定（任意） | 验证场景发送 |

---

#### `StartQueueConsumerAsync(string, Func, CancellationToken)` — 通用队列消费者

**功能：** 启动指定队列的通用消费者，内部创建独立通道，支持 async handler。

**内部步骤：**

```
① _conn.CreateModel() → channel（新通道）
② channel.BasicQos(0, 1, false)
③ _extraChannels.Add(channel) → 加入清理列表
④ new EventingBasicConsumer(channel)

回调内逻辑：
① 反序列化（零拷贝 ea.Body.Span）→ msg
② msg == null → BasicNack(tag, false, false)
③ handler(msg).GetAwaiter().GetResult() → 调用委托
   └─ true → BasicAck(tag, false)
   └─ false → BasicNack(tag, false, false)
④ catch (Exception) → BasicNack(tag, false, false) 丢弃

⑤ channel.BasicConsume(queue, autoAck:false, consumer)
⑥ await Task.Delay(-1, ct) 阻塞
```

**关键设计：**
- 不包含 Redis 幂等校验（与 StartConsumeAsync 区分）
- 通道通过 _extraChannels 跟踪，Dispose() 时统一清理
- 支持 async 委托（内部 await 异步操作）

---

#### `StartDlxConsumeAsync(Func?, CancellationToken)` — 死信队列消费者

**功能：** 消费死信队列中的消息，解析 x-death 头信息，更新 Redis 记录。

**内部步骤：**

```
① _conn.CreateModel() → _dlxChannel
② _extraChannels.Add(_dlxChannel)
③ new EventingBasicConsumer(_dlxChannel)

回调内逻辑：
第 1 步 - 反序列化（零拷贝）：
   ① Encoding.UTF8.GetString(ea.Body.Span) → json → Deserialize → msg
   ② msg == null → BasicNack 丢弃

第 2 步 - 显示消息内容：
   MsgId / Data / CreateTs 输出到控制台

第 3 步 - 解析 x-death 头：
   ① ea.BasicProperties.Headers["x-death"] → List<Dictionary>
   ② 取第一条死信记录：
      ├─ reason：死信原因（expired / rejected / maxlen）
      ├─ count：重试次数
      └─ queue：原始队列名

第 4 步 - 更新 Redis 死信记录：
   ① msgRecordSvc.GetMessageRecordAsync(msg.MsgId) → drec
   ② msgRecordSvc.UpdateStatusAsync(已进死信, drec.RetryCount)

第 5 步 - 执行业务委托（可选）：
   ① businessHandler == null → success = true（默认确认）
   ② businessHandler(msg) → 自定义处理

第 6 步 - ACK/Nack：
   ├─ success == true → BasicAck（从 DLQ 移除）
   └─ success == false → BasicNack 丢弃（不进二次死信）

④ BasicConsume(biz.dlx.queue, autoAck:false, consumer)
⑤ await Task.Delay(-1, ct)
```

---

### RedisIdempotentService.cs — 3 个方法

---

#### `TryLockMsgAsync(string, int)` — 原子加锁/幂等检查

**功能：** 使用 Lua 脚本原子检查消息是否已消费完成，未完成则加锁。

**Lua 脚本逻辑（完整流程）：**

```lua
-- KEYS[1] = mq:idempotent:{MsgId}
-- ARGV[1] = {"finished":false}
-- ARGV[2] = ttl（秒）

local val = redis.call('GET', KEYS[1])
if val then
    local ok, record = pcall(cjson.decode, val)
    -- 如果解码成功且 record.finished == true
    if ok and type(record) == 'table' and record['finished'] then
        return 0  -- 已消费完成，跳过
    end
    -- finished=false 或解码失败 → 进程崩溃遗留锁，覆盖重锁
end
redis.call('SETEX', KEYS[1], ARGV[2], ARGV[1])  -- SET + EXPIRE
return 1  -- 首次加锁成功
```

**三个分支：**

| 场景 | Redis 状态 | 返回值 | 含义 |
|------|-----------|--------|------|
| 首次消息 | key 不存在 | 1 | 可消费，已加锁 |
| 重复消息 | key 存在且 Finished=true | 0 | 跳过，已消费完成 |
| 崩溃恢复 | key 存在但 Finished=false | 1 | 覆盖重锁，重新消费 |

> **崩溃恢复场景：** 消费者在处理消息时崩溃，Finished 未改为 true，但锁未释放。下次收到同一条消息（requeue= true 重新投递）时，Lua 脚本发现 Finished=false，判定为"遗留锁"，覆盖重锁后返回 1，消息被重新消费。

---

#### `MarkFinishAsync(string)` — 标记消费完成

**功能：** 将 Redis 中幂等记录的 Finished 设为 true。

**内部步骤：**

```
① key = mq:idempotent:{msgId}
② record = { finished: true }
③ json = JsonSerializer.Serialize(record)
④ redis.StringSetAsync(key, json, when: When.Exists)
   └─ When.Exists：仅在 key 有效期内更新，key 已过期则不写入
   └─ 防止 TTL 过期后错误重建已过期的幂等记录
```

---

#### `UnLockMsgAsync(string)` — 释放锁

**功能：** 删除 Redis 幂等记录，允许消息重新消费。

**内部步骤：**

```
① key = mq:idempotent:{msgId}
② redis.KeyDeleteAsync(key)
   └─ 消费失败时调用，释放锁使消息可被重新处理
```

---

### RedisMsgRecordService.cs — 4 个方法

---

#### `CreateMessageRecordAsync(MqMessage)` — 创建消息初始记录

**内部步骤：**

```
① key = mq:record:{msg.MsgId}
② KeyExistsAsync(key) → exists
   └─ exists == true → return（不覆盖已有记录）
   └─ 防止重复发布覆盖已消费状态
③ new MessageRecord { MsgId, Data, Status="未消费", CreateTs, RetryCount=0 }
④ JsonSerializer.Serialize(record, camelCase) → json
⑤ StringSetAsync(key, json)
```

---

#### `UpdateStatusAsync(string, string, int?)` — 更新消息状态

**内部步骤：**

```
① key = mq:record:{msgId}
② StringGetAsync(key) → json
③ !json.HasValue → return（记录不存在忽略）
④ Deserialize<MessageRecord>(json) → record
⑤ record.Status = status
⑥ retryCount 有值 → record.RetryCount = retryCount
⑦ 自动记录时间戳：
   ├─ status == "已消费" 且 ConsumeTs==null → ConsumeTs = now
   └─ status == "已进死信" 且 DlqTs==null → DlqTs = now
     └─ 只记录首次时间，后续更新不覆盖
⑧ Serialize + StringSetAsync
```

---

#### `GetMessageRecordAsync(string)` — 获取单条记录

**内部步骤：**

```
① key = mq:record:{msgId}
② StringGetAsync → json
③ json.HasValue == false → return null
④ Deserialize → return MessageRecord
```

---

#### `GetAllMessageRecordsAsync()` — 获取所有记录

**内部步骤：**

```
① server.Keys(pattern: "mq:record:*") → 遍历所有消息记录 key
   └─ 通过 SCAN 命令实现（非阻塞）
② 逐个 StringGetAsync + Deserialize → List<MessageRecord>
③ return records
```

---

### Program.cs — 按 Block 逐步执行

整体采用 **3 阶段并发架构**：

```
第一阶段：初始化（Block 1）
第二阶段：启动所有消费者后台并发 → 发送所有消息（Block 2~6）
               ├─ 消费者们在后台持续处理
               └─ 主线程继续发消息，发完后 await Task.WhenAll 等待
第三阶段：60s 超时 → 全部退出 → 清理 + 验证（Block 7~9）
```

#### Block 1 — 初始化基础设施

```
① InitDelayDlxQueue() → 创建 DLX 主队列
② 循环 7 次 DeclareExQueue → 创建 6 个场景的 Ex/Queue
   ├─ 1 个 Direct（异步处理）
   ├─ 1 个 Fanout + 3 个队列（服务解耦）
   ├─ 1 个 Direct（削峰填谷）
   ├─ 1 个 Direct + 2 个队列 + TTL/DLX 参数（延迟队列）
   ├─ 1 个 Direct + 3 个队列（分布式事务）
   └─ 1 个 Fanout + 2 个队列（消息广播）
```

#### Block 2-3 — 启动基础消费者

```
① CancellationTokenSource.CreateLinkedTokenSource(lifetime.ApplicationStopping)
   └─ CancelAfter(60s) → 60 秒后自动触发取消
② StartDlxConsumeAsync → 死信消费者（dlqMsgCount 计数）
③ StartConsumeAsync → 主业务消费者（含幂等校验 + 入死信）
```

#### Block 4 — 启动 11 个场景消费者（并发）

所有消费者通过 `allConsumerTasks.Add(mqHelper.StartQueueConsumerAsync(...))` 添加到任务列表，不 await，全部后台运行。

```
① 异步处理 ×1：await Task.Delay(2000) 模拟耗时
② 服务解耦 ×3：svc_a / svc_b / svc_c 独立日志 + 计数
③ 削峰填谷 ×2：各 300ms 处理，展示进度 {cur}/20
④ 延迟队列 ×1：等待 delay.target.queue 消息
⑤ 分布式事务 ×3：order / payment / inventory 独立计数
⑥ 消息广播 ×2：sub1 / sub2 独立日志 + 计数
```

#### Block 5 — 发送 6 场景消息

```
① 异步处理：PublishToExchangeAsync("demo.async.ex", "demo.async.queue", ...)
② 服务解耦：PublishToExchangeAsync("demo.decouple.ex", "", ...)
   └─ Fanout 路由键被忽略
③ 削峰填谷：循环 20 次 PublishToExchangeAsync("demo.spike.ex", ...)
④ 延迟队列：PublishToExchangeAsync("demo.delay.ex", "demo.delay.src.queue", ...)
   └─ 5s 后自动转发到 target 队列
⑤ 分布式事务：3 次 PublishToExchangeAsync（order / payment / inventory）
⑥ 消息广播：PublishToExchangeAsync("demo.broadcast.ex", "", ...)
```

#### Block 6 — 发送原有消息 + 持久化验证

```
① 10 条唯一消息：msg_{runTag}_001 ~ 010，PublishConfirmAsync
② 5 条重复消息：复用前 5 个 MsgId，PublishConfirmAsync
③ 持久化验证：msgRecordSvc.GetMessageRecordAsync(firstMsgId)
   └─ 验证 Redis 中已存在该记录
④ await Task.WhenAll(allConsumerTasks) → 阻塞等待所有消费者完成
   └─ 60s 后 CancellationToken 触发，所有 Task.Delay(-1, ct) 抛出
   └─ OperationCanceledException 在每个消费者内部被 catch，正常退出
```

#### Block 7 — 停止 + 状态汇总

```
① stopCts.Cancel() → 确保所有消费者退出
② 再次 await Task.WhenAll 等全部结束
③ 输出 7 个计数器数值
④ mqHelper.Dispose() → 关闭所有通道和连接
⑤ host.StopAsync() → 停止 Host
```

#### Block 8 — Redis 记录验证

```
① 新建 Redis 连接 → 扫描 mq:record:*
② 筛选本次运行（msg_{runTag}）的记录 → 逐个打印 JSON
③ 全量状态分布统计：GroupBy Status → Count
④ 扫描 mq:idempotent:* → 打印每个幂等记录
⑤ 抽样验证 firstMsgId 的 Finished=true
```

#### Block 9 — 场景断言

```
逐场景检查计数器是否达到预期阈值：
① 异步处理 ≥ 1
② 服务解耦 == 3
③ 削峰填谷 ≥ 18
④ 延迟队列 ≥ 1
⑤ 分布式事务 == 3
⑥ 消息广播 == 2
```

---

## 6 大应用场景

所有场景在 Program.cs 中以独立 Block 呈现，共用 60s 超时窗口并发运行。

### 场景 1：异步处理

| 项目 | 说明 |
|------|------|
| **解决的问题** | 存在大量非核心耗时操作（发邮件、写日志、推送通知） |
| **模式** | Direct Exchange + 单一消费者 |
| **Ex/Queue** | `demo.async.ex` → `demo.async.queue` |
| **验证方式** | 主流程发一条"发 100 封邮件"任务后立即返回，消费者 2s 后台处理后确认 |
| **预期** | `asyncTaskCount >= 1` |

**验证输出：**
```
[主流程] 发送慢任务消息 → 立即返回
[消费者] 异步任务开始（2s 耗时）→ 异步任务完成 ✓
```

### 场景 2：服务解耦

| 项目 | 说明 |
|------|------|
| **解决的问题** | 多服务强依赖、迭代频繁（订单→通知/积分/物流） |
| **模式** | Fanout Exchange + 多队列独立消费 |
| **Ex/Queue** | `demo.decouple.ex` → `demo.svc_a/svc_b/svc_c.queue` |
| **验证方式** | 发送 1 条"订单已创建"事件，3 个服务各自独立接收处理 |
| **预期** | `decoupleCount == 3` |

**架构示意：**
```
          ┌→ svc_a.queue → 服务 A（通知）
发布者 ─→ Fanout ─→ svc_b.queue → 服务 B（积分）
          └→ svc_c.queue → 服务 C（物流）
```

### 场景 3：削峰填谷

| 项目 | 说明 |
|------|------|
| **解决的问题** | 大促/秒杀高并发场景，避免瞬间流量冲垮下游 |
| **模式** | Direct Exchange + 多消费者竞争（Competing Consumers） |
| **Ex/Queue** | `demo.spike.ex` → `demo.spike.queue` |
| **消费者** | 2 个，各 300ms/条处理速度 |
| **验证方式** | 急速发送 20 条秒杀订单，观察消费端逐步消化 |
| **预期** | `spikeCount >= 18` |

**效果：**
```
发送端：20 条订单瞬间全部发出
消费端：2 个消费者各 300ms/条，逐步处理 → 流量被队列削平
```

### 场景 4：延迟队列

| 项目 | 说明 |
|------|------|
| **解决的问题** | 大量定时/超时类业务（订单 30 分钟未支付自动取消、任务调度） |
| **模式** | TTL + Dead Letter Exchange（无需插件） |
| **Ex/Queue** | `demo.delay.ex` → `demo.delay.src.queue`（TTL=5s）→ `demo.delay.target.queue` |
| **验证方式** | 发送消息到 src 队列，5s 后 target 队列收到并消费 |
| **预期** | `delayCount >= 1` |

**延迟原理：**
```
发布 → src.queue（x-message-ttl=5000, x-dead-letter-exchange=demo.delay.ex）
    ↓ 5s 超时
demo.delay.ex → target.queue → 消费者接收
```

### 场景 5：分布式事务

| 项目 | 说明 |
|------|------|
| **解决的问题** | 核心交易跨多模块数据强一致 |
| **模式** | Direct Exchange + 各步骤独立队列 + 最终一致性 |
| **Ex/Queue** | `demo.tx.ex` → `demo.tx.order/payment/inventory.queue` |
| **验证方式** | 模拟下单→支付→库存 3 阶段，各步骤独立 ACK 确认 |
| **预期** | `txCount == 3` |

**架构示意：**
```
下单 → demo.tx.order.queue → [下单✓] step=1/3
支付 → demo.tx.payment.queue → [支付✓] step=2/3
库存 → demo.tx.inventory.queue → [库存✓] step=3/3
```

### 场景 6：消息广播

| 项目 | 说明 |
|------|------|
| **解决的问题** | 一对多事件通知（公告/配置刷新/数据同步） |
| **模式** | Fanout Exchange + 多订阅者 |
| **Ex/Queue** | `demo.broadcast.ex` → `demo.sub1/sub2.queue` |
| **验证方式** | 发送 1 条系统公告，2 个订阅者同时收到 |
| **预期** | `broadcastCount == 2` |

---

## 运行指南

### 前置条件

- .NET 10 SDK
- RabbitMQ 服务（默认 localhost:5672）
- Redis 服务（默认 localhost:6379）

### 修改配置

编辑 `appsettings.json`，修改以下配置为你的实际环境：

```json
{
  "RabbitConfig": {
    "Host": "你的 RabbitMQ 地址"
  },
  "RedisConfig": {
    "ConnStr": "你的 Redis 连接字符串"
  }
}
```

### 运行

```bash
cd e:\Demo\source\repos\RabbitMqDemo
dotnet run
```

### 运行流程

程序自动执行以下阶段：

```
Block 1:  初始化所有 Ex/Queue 基础设施（7 组）
Block 2:  启动死信队列消费者（后台）
Block 3:  启动主业务消费者（Redis 幂等 + 失败入死信）
Block 4:  启动 6 大场景所有消费者（后台并发，11 个消费者）
Block 5:  发送 6 大场景验证消息（5a ~ 5f）
Block 6:  发送 10 条唯一 + 5 条重复消息 + 持久化验证
          ↓（等待 60s 超时自动退出）
Block 7:  停止消费者 + 各场景统计汇总
Block 8:  验证 Redis 消息记录 + 幂等记录
Block 9:  逐场景验证结果（6 组断言）
```

### 超时控制

- 默认 60s 自动退出（`stopCts.CancelAfter(60s)`）
- 可手动 Ctrl+C 提前退出
- 所有消费者共享 CancellationTokenSource，同时优雅关闭

### 重复运行

每次运行 MsgId 基于时间戳 `yyyyMMddHHmmss` 生成，Redis 中会累积多运行的数据记录，互不干扰。

---

## 输出解读

运行结束后关注以下输出：

### Block 7 — 场景消费汇总

```
📊 场景消费汇总:
  死信队列    : 2 条          ← 本次失败的 Nack 消息数
  异步处理    : 1/1           ← 异步任务是否完成
  服务解耦    : 3/3           ← 3 个服务是否都收到
  削峰填谷    : 20/20         ← 20 条秒杀是否全部消费
  延迟队列    : 1/1           ← 5s 延迟消息是否到达
  分布式事务  : 3/3           ← 3 阶段是否全部确认
  消息广播    : 2/2           ← 2 个订阅者是否都收到
```

### Block 8 — Redis 记录

```
--- 消息记录（mq:record:*） ---
  mq:record:msg_runTag_001 => {"status":"已消费", ...}
  mq:record:spike_runTag_001 => {"status":"已消费", ...}

--- 幂等记录（mq:idempotent:*） ---
  mq:idempotent:msg_runTag_001 => {"finished":true}
```

### Block 9 — 场景断言

```
--- 9a: 异步处理 ---
✅ 异步任务已处理

--- 9b: 服务解耦 ---
✅ 1 条事件被 3 个服务独立接收

--- 9c: 削峰填谷 ---
✅ 20 条秒杀订单已逐步消费

--- 9d: 延迟队列 ---
✅ 消息经过 5s TTL 延迟后到达

--- 9e: 分布式事务 ---
✅ 3 阶段（订单→支付→库存）全部完成

--- 9f: 消息广播 ---
✅ 1 条公告被 2 个订阅者同时接收
```

---

## 依赖组件

| 组件 | 版本 | 用途 |
|------|------|------|
| RabbitMQ.Client | 6.8.1 | RabbitMQ AMQP 协议客户端 |
| StackExchange.Redis | 2.8.0 | Redis 客户端（幂等锁 + 消息记录） |
| Microsoft.Extensions.Hosting | 10.0.0 | 依赖注入 + 应用生命周期管理 |
| Microsoft.Extensions.Configuration.Json | 10.0.0 | JSON 配置文件加载 |
| .NET | 10.0 | 目标框架 |
