# Data Model: Streaming RPC（流式调用）

规格 [spec.md](./spec.md) 的关键实体落到实现层的完整模型。设计决策
见 [research.md](./research.md)（下文 D# 引用其小节）。

## 实体总览

```text
生成期                          内核运行期（core）                类型化 API（api）
─────────                      ──────────────────────          ─────────────────
MethodForm（4 值枚举）   ──►    Router 条目 {form, handler}      RegisterUnaryFor/RegisterStreamFor
生成 traits kForm        ──►    StreamCallCtx（读/写/终态）  ◄──  ServerReader/Writer/ReaderWriter
生成接口方法（4 形状）          StreamCall 状态机                 ClientReader/Writer/ReaderWriter
生成代理方法（4 形状）          StreamSend 队列（有界）           Result<Res>（arena 所有权，复用）
```

## 生成期实体

### MethodForm（枚举）

| 值 | 语义 | proto 形态 |
|----|------|-----------|
| kUnary | 一元（既有） | `Res M(Req)` |
| kServerStreaming | 服务端流 | `Res M(Req)` + `returns (stream Res)` |
| kClientStreaming | 客户端流 | `stream Req` + `Res M(stream Req)` |
| kBidi | 双向流 | `stream Req` + `stream Res` |

### 生成的 traits（`<M>Method`，扩展）

既有字段不变（ReqType/ResType/service_name/method_name/path/
ParseRequest/ParseResponse/ReqTable/ResTable），追加：

- `static constexpr ::urpc::MethodForm kForm;`——生成器从
  `MethodDescriptor::client_streaming()/server_streaming()` 推导。

### MethodDescriptor（api/service.h，扩展）

`{name, path}` 追加第三成员 `MethodForm form;`（带默认值 kUnary）：
既有二元聚合初始化 `{name, path}` 仍合法 → 003 生成物向后兼容
（FR-012）；kMethods 表生成三列。

## 内核运行期实体（core）

### Router 条目（扩展）

```text
path → { form, unary_handler | stream_handler }
```

`RegisterUnary`（既有）写 form=kUnary；`RegisterStream(path, form,
StreamHandler)` 写其余三种。Find 返回快照条目，分发按 form 构造
UnaryHandler 或 StreamCallCtx（D1）。同 path 重复注册 → 既有
ALREADY_EXISTS 语义不变（US-003 回归项）。

### StreamCallCtx（新，派生自 ServerCallCtx）

继承取消/截止时间/path 能力（IsCancelled/OnCancel/TimeRemainingMs），
新增：

| 操作 | 语义 | 触发的状态迁移 |
|------|------|--------------|
| `ReadMessage(cb)` | 注册读事件；每次调用投递一条 | 事件按序：msg… → eos=true 或非 OK 终态 |
| `WriteMessage(msg, cb)` | EncodeFrame + SendData；cb=交付完成 | Started → Streaming；队列满则挂起生产者 |
| `WriteDone()` | 服务端半关闭（无 trailers） | — |
| `Finish(st)` | SendTrailers(grpc-status)；每流一次 | → Finished |

### StreamCall 状态机（每条 HTTP/2 流一个）

```text
                    ┌────────────────────────────────────────────┐
                    │            (client RST / 断开)              │
                    ▼                                            ▼
  ┌───────┐  handler   ┌───────────┐  Finish/强制终态   ┌───────────────┐
  │ Idle  │ ─────────► │ Streaming │ ────────────────► │   Finished    │
  └───────┘            └───────────┘                   └───────────────┘
      │ handler 未完成流     │  客户端半关闭(END_STREAM)          ▲
      │ 即返回：流保持打开    │  → request_half_closed = true      │
      └─────────────────────┘   截止时间到期 → Finish(DEADLINE_   │
                                   EXCEEDED)；Shutdown 宽限超时   │
                                   → Finish(UNAVAILABLE) ────────┘
```

不变式（验证规则见下）：`Finish` 恰一次；Finished 后读/写全部拒绝；
取消即立即 Finished（对端可先于 trailers 收到 RST）。

### StreamSend 队列（客户端侧，per stream，有界）

- 深度上限 = 2（进行中 + 待发）；满时 `StreamSend` 不再推进、调用方
  在上一条 on_flushed 后继续（背压，FR-008/D4）。
- 每条目 = {framed, on_flushed, close}；`close=true` 的条目交付后
  END_STREAM（半关闭）。

### 读侧投递（服务端）

FrameDecoder 逐消息产出 → 按序匹配 ReadMessage 的 pending 回调：
- 一元兼容层为空实现（一元走既有单消息断言路径，零改动）。
- END_STREAM 时投递 `eos=true`（此后 ReadMessage 事件只可能是错误）。
- 分帧违规/超限 → 非 OK Status 终态事件 + Finish(INTERNAL)。

## 类型化 API 实体（api）

### 服务端视图（处理器运行于 loop 线程，D6）

| 类 | 形态 | 表面 | 所有权 |
|----|------|------|--------|
| `ServerWriter<Res>` | 服务端流 | `bool Write(const Res*)`（false=已取消/已终态） | 消息编码进发送队列，调用方 arena 可随即释放 |
| `ServerReader<Req>` | 客户端流 | `void ReadMessage(cb(Status, const Req*))` | 每条消息独立 arena，随回调存活；cb 返回后失效 |
| `ServerReaderWriter<Req,Res>` | 双向 | 读 + 写合成 | 同上 |

api 注册包装器：处理器返回时未写过且未 Finish → 补
`Finish(UNIMPLEMENTED)`（FR-011，对齐 003 默认语义）；异常 →
`Finish(INTERNAL)`（FR-013 贯穿流式）。

### 客户端视图（同步阻塞式；loop 线程快速失败）

| 类 | 形态 | 表面 |
|----|------|------|
| `ClientReader<Res>` | 服务端流 | `Result<Res> Read()`（eos=ok 且 value()==nullptr）；`Status Finish()` |
| `ClientWriter<Req,Res>` | 客户端流 | `bool Write(const Req*)`；`void WritesDone()`；`Result<Res> Finish()` |
| `ClientReaderWriter<Req,Res>` | 双向 | Read + Write + WritesDone + `Status Finish()` |

同步实现 = 每操作一个 promise/future 桥接到 core 事件（复用一元同步
Call 的模式）；`Read()` 的 `Result<Res>` 复用一元 arena 所有权（消息
随 Result 存活）。

### 生成接口/代理形状

见 [contracts/streaming-interface.md](./contracts/streaming-interface.md)
第 2/3 节（签名逐字契约）。

## 关键时序（三形态线协议序列，gRPC 兼容）

```text
服务端流:  C: HEADERS(POST :path) DATA(req, END_STREAM)
           S: HEADERS(200) DATA(msg1)…DATA(msgN) TRAILERS(grpc-status:0)
客户端流:  C: HEADERS(POST :path) DATA(m1)…DATA(mN, END_STREAM)
           S: HEADERS(200) DATA(resp) TRAILERS(grpc-status:0)
双向流:    C: HEADERS(POST :path) DATA…（可迟于 S 首条响应）
           S: HEADERS(200) DATA…（独立于请求侧结束时机）
           双方各自 END_STREAM；S: TRAILERS 终态
错误捷径:  S: HEADERS(200 + grpc-status + END_STREAM)（trailers-only，
           沿用 003 修复的 SendTrailersOnly）
```

## 验证规则汇总（映射 FR）

| 规则 | 验证点 | FR |
|------|--------|----|
| 顺序投递 | e2e 断言 N 条消息内容/顺序逐条相等 | FR-007 |
| Finish 恰一次 | core 单测：二次 Finish 返回错误且线上无第二 trailers | FR-009 |
| 半关闭后仍可收 | 双向 e2e：客户端 WritesDone 后继续收齐服务端消息 | FR-004/US3 |
| 零消息流 | 服务端流 0 条响应、客户端流 0 条请求，双方正常终态 | FR-002/003 |
| 有界发送 | 慢消费者 + 10k 消息：发送队列深度 ≤ 上限，内存平稳 | FR-008/SC-004 |
| 超时覆盖全程 | 流中途到期 → 双方资源释放，状态 DEADLINE_EXCEEDED | FR-010 |
| 未注册/异常 | 未注册流方法 → UNIMPLEMENTED；抛异常 → INTERNAL 且服务端存活 | FR-011 |
| 一元零回归 | 既有全部测试不改动不跳过 | FR-015/SC-006 |
| 生成兼容 | 003 时代二元 kMethods 初始化 + 一元接口编译不变 | FR-012 |
