# Phase 0 Research: Streaming RPC（流式调用）

本文件记录将规格未知项转为实现决策的研究结论。格式：决策 → 理由 →
备选方案。规格（spec.md）无 [NEEDS CLARIFICATION] 遗留；本阶段的
"未知"来自技术形态选择：内核如何承载多消息、类型化 API 长什么样、
生成器如何按形态展开。

## 1. 内核统一模型：StreamCallCtx + 按形态注册（router 扩展）

**Decision**: `core::ServerCallCtx`（一元，既有）保持不变；新增派生类
`core::StreamCallCtx : public ServerCallCtx`，追加四个流式操作；路由
新增 `RegisterStream(path, MethodForm, StreamHandler)`，与既有
`RegisterUnary` 并列。`Find` 返回统一条目 `{form, unary|stream}`，分
发路径按形态构造对应 ctx。

```cpp
enum class MethodForm { kUnary, kServerStreaming, kClientStreaming, kBidi };

class StreamCallCtx : public ServerCallCtx {
 public:
  // 请求侧（客户端→服务端）：每次调用投递一个事件；流半关闭后投递
  // 恰好一个 eos=true 终态事件；分帧违规投递非 OK Status。严格按序。
  virtual void ReadMessage(
      std::function<void(Status st, bool eos, std::string msg)> cb) = 0;
  // 响应侧（服务端→客户端）：入参为「未加帧」的消息体；实现负责
  // EncodeFrame + SendData。cb 在消息已全部交付传输层后触发
  // （背压边界，FR-008；取消/流关闭后以非 OK 触发）。
  virtual void WriteMessage(std::string msg,
                            std::function<void(Status)> cb) = 0;
  // 服务端半关闭响应侧（不再发送消息）。
  virtual void WriteDone() = 0;
  // 终态：发送 trailers（grpc-status/grpc-message）。隐含 WriteDone；
  // 每个流恰可调用一次，此后写被拒。
  virtual void Finish(Status st) = 0;
};

using StreamHandler = std::function<void(StreamCallCtx& call)>;
```

**Rationale**:
- 一元热路径零改动（FR-015 零回归）：既有 UnaryHandler/Respond 调度
  原样保留，流式是纯增量。
- 一元 = 流式的单消息特例（spec 关键实体）：core 语义统一，api 层再
  分类型化形状。
- 回调式 ReadMessage/WriteMessage 与事件循环驱动模型一致（不阻塞
  loop 线程，001 既定决策），也是未来 C-ABI 绑定的天然边界（原则 II：
  回调 + 字节串，无 STL 泄漏形态）。

**Alternatives**:
- 直接把四个操作加进 ServerCallCtx（被否：一元处理器被迫面对无关
  接口；一元分发路径要为此买单）。
- 处理器同步拉取式 API（`bool NextMessage(std::string*)`）（被否：
  拉取式要求处理器线程阻塞等待消息，与「不阻塞事件循环」根本冲突；
  需要为每个处理器分配独立线程，代价不可接受）。

## 2. 服务端分发与生命周期：处理器返回 ≠ 流结束

**Decision**: 流式处理器在投递到 loop 线程时同步调用一次（与一元一
致）；处理器**返回后流保持打开**——它可以持有 `StreamCallCtx&`（或
其类型化包装）继续在后续 loop 回调里读写。流的终态只有三种：处理器
调用 `Finish`、对端取消/断开（FireCancel 路径）、服务端关闭/截止时间
强制结束。api 层类型化注册包装器对「同步风格处理器返回时既未写也未
Finish」的调用补发 UNIMPLEMENTED（对齐 003 的默认未实现语义，FR-011）。

**Rationale**: 流式处理器的生命周期天然长于一次调用栈（US3：双方独立
收发）；「返回即结束」的一元假设不成立，必须显式建模。
**Alternatives**: 处理器必须阻塞到流结束（被否：阻塞 loop 线程，001
明确的反模式）。

## 3. 请求侧收流：FrameDecoder 已原生支持多消息

**Decision**: 请求侧不改 codec。`FrameDecoder` 本就是增量式多消息解
码器（`Consume`/`TakeMessage`/`message_count`，codec.h）。服务端流式
分发变更仅为：收到 END_STREAM 之前**不**触发 FinishRequest 式的「完
整请求」断言，而是把解码出的每条消息按序交给处理器（ReadMessage 的
pending 回调队列）；END_STREAM 到达时投递 eos=true。

**Rationale**: 001 选用增量解码器是为超大消息跨帧拆分设计的，多消息
是其自然延伸，零新代码。
**Alternatives**: 为流式新写解码器（被否：重复造轮子，且丧失「单条
消息大小上限」的既有实现与测试背书，FR-007）。

## 4. 响应侧发流：复用 SendData/SendTrailers，消息级背压回调

**Decision**: 服务端响应侧完全复用既有 H2 原语：每条响应消息 =
`EncodeFrame` + `SendData(sid, framed, false)`；结束 = `Finish` →
`SendTrailers({grpc-status, grpc-message})`；trailers-only 错误 =
既有 `SendTrailersOnly`（003 修复过的路径）。`WriteMessage` 的 cb 在
该消息的 DATA 帧序列化完成后触发：实现上以 per-stream 的发送队列承
载（nghttp2 data provider 序列化完毕即触发队首 cb），队列深度有界
（见 data-model 状态机），写满时 WriteMessage 返回排队但不再推进生产
者——这是 FR-008「内存有界」的框架侧保证；配合消费者窗口，端到端
背压成立。

**Rationale**: HTTP/2 流控窗口是唯一正确的背压机制（gRPC 同款语义）；
消息级回调把「可以安全生产下一条」的信号交给处理器，内存上界 =
窗口 + 单条消息上限。
**Alternatives**:
- 一次性把全部响应提交给 nghttp2（被否：nghttp2 内部缓冲不受我们控
  制，慢消费者场景内存无界，违反 FR-008/SC-004）。
- v1 不做背压（被否：规格 FR-008 为 MUST）。

## 5. 客户端核心原语：OpenStream / StreamSend / StreamCloseSend

**Decision**: core::Channel 新增三条流式原语（与既有 Call/CallAsync
并列，复用同一 by_stream/calls/超时/取消机制）：

```cpp
struct StreamEvents {
  std::function<void(Status, std::string)> on_message;  // 响应消息（未加帧）
  std::function<void(Status)> on_complete;              // 终态（trailers 或错误）
};
// 发送响应头、开启 HTTP/2 流（请求侧不半关闭）。返回流句柄（call id）。
uint64_t OpenStream(const std::string& path, StreamEvents events,
                    uint64_t timeout_ms);
// 发送一条请求消息；close=true 时本消息之后立即 END_STREAM（半关闭）。
// on_flushed 在消息交付传输层后触发（背压边界，同 #4）。
void StreamSend(uint64_t id, std::string framed,
                std::function<void(Status)> on_flushed, bool close);
// 不再发送更多请求消息（空 DATA + END_STREAM）。
void StreamCloseSend(uint64_t id);
```

客户端接收路径扩展：响应初始头（:status 200，无 grpc-status）→ 进入
「接收中」；DATA 解码出的每条消息 → `on_message`；trailers 或
TrailersOnly → `on_complete`（既有 grpc-status 解析复用）。超时计时
器、`Cancel`（RST_STREAM）、连接断开处理全部复用一元既有机制。

**Rationale**: 客户端流式 = 一元的「多条消息 + 显式半关闭」推广；复
用既有调用表意味着超时/取消/连接丢失语义与一元完全一致（FR-009/010）。
**Alternatives**: 独立的 StreamChannel 类（被否：与一元并存两套超时/
取消/重连语义，维护成本翻倍）。

**h2_session 增量**：`SubmitRequestOpen(...)`——与 `SubmitRequest` 相
同的头序列，但提交头后不半关闭（`nghttp2_submit_request2` 传非空但空
载的 provider 会直接 END_STREAM，故用 `nghttp2_submit_headers` 开
流，body 后续经 `nghttp2_submit_data2` 逐条提交）。半关闭 = 空载荷
provider + END_STREAM 的 submit_data2（等价 gRPC 客户端的
halfClose）。

## 6. 类型化 API：gRPC-C++ 风格同步 Reader/Writer（禁 loop 线程）

**Decision**: `libs/api/include/urpc/stream.h` 新增六个类型化包装 +
detail 桥接（`libs/api/include/urpc/detail/raw.h` 增补流式注册/调用
声明）：

```cpp
// 服务端（处理器在 loop 线程运行：写即排队，读为回调桥接）
template <typename Res> class ServerWriter {           // 服务端流
 public:  bool Write(const Res* msg);  void Finish(Status);  // Finish 由包装器代管
};
template <typename Req, typename Res>                  // 客户端流
class ServerReader {
 public:  // 每次注册一个回调；消息按序触发；eos/错误终态一次
   void ReadMessage(std::function<void(Status, const Req*)> cb);
};
template <typename Req, typename Res>                  // 双向流
class ServerReaderWriter : /* ServerReader + ServerWriter 合成 */ {};

// 客户端（同步阻塞式；loop 线程上调用立即快速失败，沿用一元 FR-002）
template <typename Res> class ClientReader {           // 服务端流
 public:  Result<Res> Read();        // eos: ok 且 value()==nullptr
          Status Finish();           // 阻塞至 trailers
};
template <typename Req, typename Res> class ClientWriter {  // 客户端流
 public:  bool Write(const Req*);
          void WritesDone();
          Result<Res> Finish();      // 收响应消息 + trailers
};
template <typename Req, typename Res> class ClientReaderWriter {  // 双向
 public:  Result<Res> Read();  bool Write(const Req*);
          void WritesDone();  Status Finish();
};
```

生成的服务端接口方法签名（`I<Service>`，未重写默认 = UNIMPLEMENTED）：

```cpp
// 服务端流：Range(请求) → 响应序列
virtual void Range(::urpc::ServerContext& ctx, const Req* request,
                   ::urpc::ServerWriter<Res>& writer) { (void)ctx; /*auto*/ }
// 客户端流：Accumulate(请求序列) → 单响应
virtual void Accumulate(::urpc::ServerContext& ctx,
                        ::urpc::ServerReader<Req>& reader,
                        ::urpc::UnaryDone<Res> done) { (void)...; }
// 双向流
virtual void Chat(::urpc::ServerContext& ctx,
                  ::urpc::ServerReaderWriter<Req, Res>& stream) { (void)...; }
```

生成的客户端代理方法：

```cpp
std::unique_ptr<::urpc::ClientReader<Res>>
    Range(const Req* request, uint64_t timeout_ms);
std::unique_ptr<::urpc::ClientWriter<Req, Res>>
    Accumulate(uint64_t timeout_ms);
std::unique_ptr<::urpc::ClientReaderWriter<Req, Res>>
    Chat(uint64_t timeout_ms);
```

**Rationale**:
- 同步阻塞式与 gRPC-C++ 心智一致（SC-005：流式业务代码量与一元相当）；
  「禁止在事件循环线程内使用同步接口」沿用 001 决策，`Client` 侧经
  既有 `ChannelOnLoopThread` 快速失败。
- 读侧返回 `Result<Res>` 复用一元的 arena 所有权模型（消息随 Result
  存活，零拷贝交接）；eos = ok 且无值，与「零响应合法」（US1 场景 2）
  自然统一。
- 服务端 ServerWriter::Write 同步返回 bool（失败=已取消/已关闭）：处
  理器本来就跑在 loop 线程上，写即入队，无需阻塞原语；内核侧背压由
  #4 的消息级队列保证。

**Alternatives**:
- 客户端回调式（async）为主（被否：v1 类型化面以「gRPC-C++ 手感」为
  目标；异步原语已在 core 暴露，未来绑定层直接消费，原则 II 演进空
  间不受损）。
- 服务端 Write 带完成回调（推迟：处理器是同步代码，回调式反而逼出状
  态机；队列有界已满足 FR-008）。

## 7. 生成器：按 MethodForm 展开签名

**Decision**: 撤除 v1 的「streaming methods are not supported」拒绝
（urpc_codegen.cc），改为按
`MethodDescriptor::client_streaming()/server_streaming()` 分派四种生
成模板：traits 结构体追加 `static constexpr ::urpc::MethodForm kForm`；
接口/代理/注册按 #6 的签名表展开；`kMethods` 表新增第三列 `form`
（`MethodDescriptor` 追加带默认值的成员，既有二元聚合初始化仍合法 =
字节级向后兼容，FR-012）。`RegisterService` 按列分派到
`RegisterUnaryFor` / `RegisterStreamFor`。

**Rationale**: 003 已确立「描述变更 → 编译期提醒」的生成契约（SC-002
同源），流式必须走同一条通道，否则出现两套编程模型。
**Alternatives**: 手写流式服务注册（被否：同 003 对宏/手写的否决理由）。

## 8. 互操作与基准

**Decision**:
- `examples/streaming/`：新端到端示例（server + client，三形态各一
  方法：`Sum` 客户端流、`Range` 服务端流、`Chat` 双向流），供
  quickstart、e2e、互操作 B 象限复用（沿用 examples/echo 的构建模式，
  urpc_proto_upb 一体生成）。
- `interop/python/peer_client.py`（官方→urpc）扩展：Range 读 5 条按
  序断言；Sum 上行 10 条取回合计；Chat 交错 5 轮断言双方序列。
- `interop/python/peer_server.py`（官方→对端）实现同名三方法；urpc
  客户端示例新增 `--streaming` 模式驱动它。
- `run_interop.py` 增加流式象限（仍保留 grpcio 缺失时 SKIP 语义）。
- `bench_streaming.cc`（libs/api/bench/）：server-streaming 单流吞吐
  + bidi 往返时延，`LABELS bench`，纳入 `ctest --preset bench`。

**Rationale**: 宪法 I（互操作为发布门禁）+ IV（三层次测试、基准统一
Google Benchmark）；官方 gRPC Python 对端是 001 已验收的互操作形态。
**Alternatives**: 仅自测（被否：流式线协议细节多——END_STREAM 时机、
半关闭后继续收——只有官方实现能背书兼容性）。

## 9. 截止时间、取消与关闭语义的复用

**Decision**: 截止时间/取消/优雅关闭全部复用一元既有机制，不新增平
行实现：客户端 `timeout_ms` 覆盖整条流生命周期（到 Finish 为止，超时
→ RST_STREAM → 服务端 OnStreamClose → FireCancel）；服务端 per-call
deadline 到期 → `Finish(DEADLINE_EXCEEDED)`（复用 003 修复的
SendTrailersOnly 无守卫发送路径）+ 中止读侧；`Server::Shutdown` 排空
在途流（宽限期内等 Finish，超时强制 RST）——Conn 排空逻辑把流式调用
视为「在途」即可。

**Rationale**: US6 全部场景都是一元语义的超集；统一实现 = 统一测试面。
**Alternatives**: 流式独立的取消/超时通道（被否：语义漂移风险，违反
「一元是流式特例」的模型统一性）。
