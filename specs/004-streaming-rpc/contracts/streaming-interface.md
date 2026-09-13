# Contracts: Streaming Interface（流式调用接口契约）

本契约约束三件事：proto → 生成物的展开规则（§1–3）、内核原语语义
（§4）、错误与边界行为（§5）。一元契约见
[003 service-interface.md](../003-typed-service-interface/contracts/service-interface.md)，
本文件只描述增量；冲突时以本文件为准（流式维度）。

示例贯穿使用 `examples/streaming/streaming.proto`：

```proto
package example;
service StreamService {
  rpc Range(SumRequest) returns (stream SumResponse);            // 服务端流
  rpc Sum(stream AddRequest) returns (TotalResponse);            // 客户端流
  rpc Chat(stream ChatMsg) returns (stream ChatMsg);             // 双向流
}
```

## 1. MethodForm 判定（生成器）

| client_streaming() | server_streaming() | MethodForm |
|--------------------|--------------------|------------|
| false | false | kUnary（走 003 通道，本契约不触及） |
| false | true  | kServerStreaming |
| true  | false | kClientStreaming |
| true  | true  | kBidi |

v1 的「streaming methods are not supported」拒绝路径移除。

## 2. 生成服务端接口（`IStreamService`，用户继承实现）

每方法按形态生成可重写虚函数，**均带 UNIMPLEMENTED 兜底默认体**：
处理器返回时未写过且未 Finish → 包装器补发 UNIMPLEMENTED（FR-004/011）。

```cpp
// 服务端流：单请求 → 由 writer 产出响应序列；结束态由包装器在
// 处理器返回后代发（OK / 用户经 writer.Finish 指定状态）
virtual void Range(::urpc::ServerContext& ctx,
                   const example_SumRequest* request,
                   ::urpc::ServerWriter<example_SumResponse>& writer);

// 客户端流：经 reader 消费请求序列；done 结束调用（恰好一次）
virtual void Sum(::urpc::ServerContext& ctx,
                 ::urpc::ServerReader<example_AddRequest>& reader,
                 ::urpc::UnaryDone<example_TotalResponse> done);

// 双向流：stream 同时提供读侧回调注册与写侧
virtual void Chat(::urpc::ServerContext& ctx,
                  ::urpc::ServerReaderWriter<example_ChatMsg,
                                             example_ChatMsg>& stream);
```

### 类型化包装器契约（api/stream.h）

```cpp
template <typename Res>
class ServerWriter {
 public:
  // 编码并排队发送一条响应；返回 false = 流已取消/已终态（此后写全部失败）。
  // 返回 true = 消息已进入有界发送队列（交付由框架完成，FR-008）。
  bool Write(const Res* msg);
  // 以非 OK 状态提前终止（至多一次）；处理器正常返回时由包装器代发 OK。
  bool Finish(Status st);
};

template <typename Req, typename Res>
class ServerReader {
 public:
  // 注册读回调（同一时刻至多一个挂起请求）；消息严格按序触发。
  // 终态语义：eos=true 时 msg 为空且 st.ok()（客户端已半关闭）；
  // 分帧违规/取消 → st 非 ok。每条消息的 arena 随回调存活。
  void ReadMessage(std::function<void(Status st, bool eos, const Req* msg)> cb);
};

template <typename Req, typename Res>
class ServerReaderWriter : private ServerReader<Req>, private ServerWriter<Res> {
 public:
  using ServerReader<Req>::ReadMessage;
  using ServerWriter<Res>::Write;
  using ServerWriter<Res>::Finish;
};
```

### 注册契约（扩展 003 的 RegisterService）

```cpp
inline ::urpc::Status RegisterService(::urpc::Server& server,
                                      IStreamService& impl);
```

按 kMethods 的 form 列分派：kUnary → RegisterUnaryFor（既有）；其余 →
`RegisterStreamFor<M>(server, service, method, handler)`（新，unary.h
同构的 header-inline 模板）。异常包容（FR-013）与重复注册拒绝（003）
语义逐条沿用。

## 3. 生成客户端代理（`StreamServiceProxy : public IStreamService`）

业务面入口（构造/连接管理契约与 003 完全一致：weak channel、
Client::Proxy<P>() 工厂、无连接成员）：

```cpp
// 服务端流：发送单请求并半关闭，返回读端
std::unique_ptr<::urpc::ClientReader<example_SumResponse>>
Range(const example_SumRequest* request, uint64_t timeout_ms);

// 客户端流：返回写端；Finish() 收取唯一响应
std::unique_ptr<::urpc::ClientWriter<example_AddRequest, example_TotalResponse>>
Sum(uint64_t timeout_ms);

// 双向流
std::unique_ptr<::urpc::ClientReaderWriter<example_ChatMsg, example_ChatMsg>>
Chat(uint64_t timeout_ms);
```

### 客户端类型契约（api/stream.h）

```cpp
template <typename Res>
class ClientReader {                       // 服务端流
 public:
  // 阻塞读下一条；eos → ok 且 value()==nullptr（零响应合法）；
  // 流错误 → 非 ok Status。禁止在 urpc 事件循环线程调用（快速失败）。
  Result<Res> Read();
  Status Finish();                         // 阻塞至 trailers；返回终态
};

template <typename Req, typename Res>
class ClientWriter {                       // 客户端流
 public:
  bool Write(const Req*);                  // false = 流已死（取消/错误/关闭）
  void WritesDone();                       // 半关闭请求侧（阻塞至已交付）
  Result<Res> Finish();                    // 阻塞至收到唯一响应 + trailers
};

template <typename Req, typename Res>
class ClientReaderWriter {                 // 双向流
 public:
  Result<Res> Read();
  bool Write(const Req*);
  void WritesDone();
  Status Finish();                         // 阻塞至 trailers（无终态消息）
};
```

线程契约：同一流的并发 `Write` 须外部串行化；`Read` 可与 `Write`/
`WritesDone` 并行（双向流的核心用法，US3）。

## 4. 内核原语契约（core，字节级）

| 原语 | 契约 |
|------|------|
| `RegisterStream(path, form, handler)` | path 重复 → ALREADY_EXISTS；form ∈ {kServerStreaming, kClientStreaming, kBidi} |
| `StreamCallCtx::ReadMessage(cb)` | 事件严格按序；至多一个挂起回调；终态后注册 → 立即以终态触发 |
| `StreamCallCtx::WriteMessage(msg, cb)` | msg 为未加帧载荷；cb 恰一次（ok=已交付传输层；非 ok=取消/断开/终态后写） |
| `StreamCallCtx::Finish(st)` | SendTrailers(grpc-status/grpc-message)；二次调用 → 错误 Status，线上无第二 trailers |
| `Channel::OpenStream(path, events, timeout_ms)` | 开流（请求头不带 END_STREAM）；events.on_message 按序、on_complete 恰一次；超时覆盖整流 |
| `Channel::StreamSend(id, framed, cb, close)` | close=true 的条目交付后 END_STREAM；队列满时挂起至前一条 on_flushed |
| `Channel::StreamCloseSend(id)` | 空 DATA + END_STREAM（幂等于 close=true 的尾随 Send） |
| `Channel::Cancel(id)` | 复用一元：RST_STREAM；两侧回调以取消终态收尾 |

线协议序列（逐字节语义）见 [data-model.md](./data-model.md)「关键时序」。

## 5. 错误与边界行为汇总

| 场景 | 行为 |
|------|------|
| 未注册流式方法 | TrailersOnly UNIMPLEMENTED（复用 001/003 路径） |
| 处理器异常 | Finish(INTERNAL)；连接与其他流不受影响（FR-013） |
| 客户端流：服务端提前 Finish | 服务端半关闭；客户端后续消息触发流重置；客户端 Write 返回 false，不悬挂（US2 场景 2） |
| 服务端流：客户端中途取消 | 服务端 Write 返回 false；处理器 OnCancel 观察；框架停发（US6 场景 1） |
| 截止时间中途到期 | Finish(DEADLINE_EXCEEDED) + 读侧终态；双方资源释放（FR-010） |
| 零消息流 | 服务端流 0 条响应 → 客户端 Read 立即 eos；客户端流 0 条请求 → 处理器消费到 eos（FR-002/003） |
| 单条超大消息 | 沿用 max_receive_message_size（客户端经 Client::Options 生效，含 003 修复的传播）；跨 DATA 帧正确重组（FR-007） |
| 优雅关闭 | 在途流计入排空；宽限期到 → Finish(UNAVAILABLE) + RST（FR-016） |
| 一元零回归 | UnaryHandler/Router/RegisterUnary 路径零改动（FR-015） |
