# Tasks: Streaming RPC（流式调用：服务端流 / 客户端流 / 双向流）

**Input**: Design documents from `/specs/004-streaming-rpc/`

**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅,
contracts/streaming-interface.md ✅, quickstart.md ✅

**Tests**: 纳入（spec FR-014 明确要求三层自动化验证 + Google Benchmark
基准；各故事内先写测试再实现，确认失败后实现转绿）。

**Organization**: 按用户故事分组（US1–US7 对应 spec.md 的 7 个故事，
优先级 P1→P3）。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行（不同文件、无未完成依赖）
- **[Story]**: 所属用户故事（US1–US7）
- 描述中给出确切文件路径

## 实施纪律（本仓库环境约束，适用于所有任务）

- 新建 C++ 源文件一律使用 `.cc` 扩展名（既有 `libs/**` 已统一）；
  修改现有文件时保持其当前扩展名。
- include 完整性按 GCC 严格标准执行（`std::size`→`<iterator>`、
  `std::move/declval`→`<utility>` 等）；嵌套结构体禁用 NSDMI + 外围
  类默认实参组合（GCC/Clang 拒绝，见 003 的 client.h 修复）。
- 每完成一个 Phase 用本机 `mingw g++ -fsyntax-only` 全 TU 扫描 +
  MSVC 构建/测试双验证后再进入下一阶段。
- 错误路径必须真实发响应：不得在设置「已响应」守卫后调用带守卫的
  发送包装（003 的 Respond 教训）；生成代码的 catch 块不得引用被
  move 走的对象。

---

## Phase 1: Setup（共享基础设施）

**Purpose**: 流式 proto 与构建骨架、形态枚举与生成器解封

- [x] T001 创建 `examples/streaming/streaming.proto`：package example，
  service StreamService 含三形态方法 Range(单请求→stream SumResponse)、
  Sum(stream AddRequest→单响应)、Chat(stream ChatMsg→stream ChatMsg)；
  新建 `examples/streaming/CMakeLists.txt`（urpc_proto_upb 一体生成 +
  server/client 两个可执行目标骨架，模式照抄 examples/echo）并接入
  顶层 CMake
- [x] T002 [P] 定义 `MethodForm` 枚举（kUnary/kServerStreaming/
  kClientStreaming/kBidi）：`libs/api/include/urpc/service.h` 的
  MethodDescriptor 追加带默认值 kUnary 的 `form` 成员（保持二元聚合
  初始化合法 = 003 生成物兼容，FR-012）；`libs/core/include/urpc/core/
  router.h` 同步声明 core 侧 MethodForm；`generator/urpc_codegen.cc`
  撤除「streaming methods are not supported」拒绝——改为流式方法仅
  生成 traits（含 `static constexpr MethodForm kForm`）+ kMethods 表
  三列，接口/代理方法暂不生成（US4 交付）；更新
  `libs/api/test/test_service_codegen.cc` 断言 kMethods 三列

**Checkpoint**: streaming.proto 可构建（生成 traits+kMethods），既有
全部测试零回归

---

## Phase 2: Foundational（阻塞前置，所有故事依赖）

**Purpose**: 内核多消息机制——服务端流式调用上下文、客户端流式原语

**⚠️ CRITICAL**: 未完成本阶段，任何用户故事不得开始

- [x] T003 `libs/core/include/urpc/core/router.h` + `libs/core/source/
  urpc_router.cpp`：`RegisterStream(path, MethodForm, StreamHandler)`
  （path 重复 → ALREADY_EXISTS）；Find 返回 `{form, handler}` 统一条
  目；一元路径零改动（FR-015）
- [x] T004 `libs/core/include/urpc/core/`（StreamCallCtx：ReadMessage/
  WriteMessage/WriteDone/Finish，签名按 contracts §4）+ `libs/core/
  source/urpc_server.cc` 流式分发：每流一个调用状态（Idle→Streaming→
  Finished）；请求侧 FrameDecoder 逐消息按序投递、END_STREAM 投递
  eos=true；响应侧 WriteMessage=EncodeFrame+SendData，per-stream
  有界发送队列（深度 2，research.md D4）+ 交付回调；Finish=
  SendTrailers 恰一次；deadline 到期 → Finish(DEADLINE_EXCEEDED)；
  OnStreamClose → FireCancel；Shutdown 把流式调用计入排空（FR-016）
- [x] T005 客户端原语：`libs/core/include/urpc/core/channel.h` +
  `libs/core/source/urpc_channel.cc` 新增 OpenStream(path, events,
  timeout_ms)/StreamSend(id, framed, cb, close)/StreamCloseSend(id)
  （签名按 contracts §4；复用 calls/by_stream/超时/Cancel 机制，
  research.md D5）；`libs/core/include/urpc/core/h2_session.h` +
  `libs/core/source/urpc_h2_session.cc` 新增 SubmitRequestOpen（开流
  不半关闭）；客户端接收路径：DATA 解码消息 → on_message 按序、
  trailers/TrailersOnly → on_complete 恰一次
- [x] T006 新建 `libs/core/test/test_streaming.cc`：内存 H2 对驱动的
  内核级测试（先写、确认失败）：三形态非类型化往返、消息顺序、eos
  语义、Finish 恰一次、终态后读写拒绝、零消息流、有界发送队列深度
  ≤2（慢消费者）、未注册流方法 → UNIMPLEMENTED；接入
  `libs/core/CMakeLists.txt`

**Checkpoint**: 内核三形态非类型化闭环可用，test_streaming.cc 全绿，
既有测试零回归

---

## Phase 3: User Story 1 - 服务端流式方法 (Priority: P1) 🎯 MVP

**Goal**: 客户端发一条请求，服务端经 ServerWriter 按序产出 N≥0 条强
类型响应，流以 OK 或错误状态结束

**Independent Test**: 仅注册 Range 的服务端 + 客户端读端，验证按序
收 5 条 / 零响应 / 中途错误三种场景

### Tests for User Story 1 ⚠️（先写，确认失败）

- [x] T007 [US1] 新建 `libs/api/test/test_streaming.cc`：ServerStreaming
  e2e 用例——按序收 5 条断言逐条相等（FR-007）、零响应流、处理器
  中途 Finish(UNAVAILABLE) 后客户端收到部分消息+错误状态、后继调用
  服务端存活（FR-013）；接入 `libs/api/CMakeLists.txt`

### Implementation for User Story 1

- [x] T008 新建 `libs/api/include/urpc/stream.h`：`ServerWriter<Res>`
  （bool Write(const Res*) / bool Finish(Status)，语义按 contracts
  §2——处理器运行于 loop 线程，写即入队）+ 注册适配器
  `Server::RegisterServerStreamingFor<M>(service, method, handler)`
  （header-inline，走 detail 桥接，异常 → Finish(INTERNAL)，处理器
  返回未终态 → 补发 OK，未写未终态 → UNIMPLEMENTED，FR-011）
- [x] T009 [US1] `libs/api/source/urpc_api_impl.cc` + `libs/api/
  include/urpc/detail/raw.h`：流式 detail 桥接（StreamRegisterRaw、
  ChannelOpenStreamRaw）；`ClientReader<Res>` 同步 Read/Finish
  （promise/future 桥接 core 事件；loop 线程快速失败；Result<Res>
  复用一元 arena 所有权，eos=ok 且 value()==nullptr）
- [x] T010 [US1] 示例接线：`examples/streaming/streaming_server_main.*
  `（手写 traits 注册 Range：产出 0..N-1 序列）与
  `streaming_client_main.*`（`--range N` 模式：ClientReader 按序打印）
- [x] T011 [US1] 运行 T007 全绿 + `ctest --test-dir build -R streaming`
  （examples e2e 接入，若无则手动双进程验证并记录）

**Checkpoint**: US1 独立可用——服务端流端到端可演示（MVP）

---

## Phase 4: User Story 2 - 客户端流式方法 (Priority: P1)

**Goal**: 客户端连续发送 N≥0 条请求后半关闭，服务端经 ServerReader
逐条消费后返回唯一响应

**Independent Test**: 仅注册 Sum 的服务端 + 客户端写端：上行 10 条取
合计、零请求即半关闭、服务端提前 Finish 三场景

### Tests for User Story 2 ⚠️

- [x] T012 [US2] `libs/api/test/test_streaming.cc` 追加 ClientStreaming
  用例：10 条消息聚合值断言、空流（0 条即 WritesDone）、服务端在读
  完前 done() 提前结束后客户端 Write 返回 false 且不悬挂（US2 场景
  2）、处理异常 → INTERNAL（FR-013）

### Implementation for User Story 2

- [x] T013 [US2] `libs/api/include/urpc/stream.h`：`ServerReader<Req>`
  （ReadMessage(cb(Status, bool eos, const Req*))，消息 arena 随回调
  存活）+ `ClientWriter<Req,Res>`（bool Write / void WritesDone /
  Result<Res> Finish）+ `RegisterClientStreamingFor<M>` 适配器（语义
  同 T008 包装器）
- [x] T014 [US2] `libs/api/source/urpc_api_impl.cc`：ClientWriter 的
  detail 桥接（StreamSend 序列 + StreamCloseSend + 终态等待）
- [x] T015 [US2] 示例接线：server 实现 Sum（求和返回）+ client
  `--sum N` 模式；追加 T012 对应 e2e 验证

**Checkpoint**: US1+US2 独立可用

---

## Phase 5: User Story 3 - 双向流式方法 (Priority: P2)

**Goal**: 同一调用上双方独立收发；半关闭一侧后仍可继续收另一方消息

**Independent Test**: Chat 交错 5 轮：客户端 Write→服务端逐条回显，
双方独立收尾，序列完整无死锁（US3 场景 1/2）

### Tests for User Story 3 ⚠️

- [x] T016 [US3] `libs/api/test/test_streaming.cc` 追加 Bidi 用例：
  交错 5 轮回显顺序断言、客户端 WritesDone 后继续收齐服务端剩余
  消息（半关闭 ≠ 流结束，FR-004）、读侧与写侧并发（US3 线程契约）

### Implementation for User Story 3

- [x] T017 [US3] `libs/api/include/urpc/stream.h`：
  `ServerReaderWriter<Req,Res>`（ServerReader+ServerWriter 合成）+
  `ClientReaderWriter<Req,Res>`（Read/Write/WritesDone/Finish(Status)）
  + `RegisterBidiFor<M>` 适配器
- [x] T018 [US3] `libs/api/source/urpc_api_impl.cc`：合成类的 detail
  桥接（复用 T009/T014 桥接原语）
- [x] T019 [US3] 示例接线：server 实现 Chat（逐条回显）+ client
  `--chat N` 模式；追加 T016 对应 e2e 验证

**Checkpoint**: 三形态全部独立可用

---

## Phase 6: User Story 4 - 工具生成流式类型化接口 (Priority: P2)

**Goal**: proto `stream` 方法自动生成接口/代理/注册（contracts §2–3
逐字签名）；一元生成物字节级不变

**Independent Test**: 对 streaming.proto 生成后，仅编写「继承
IStreamService + RegisterService」的服务端与「StreamServiceProxy」
客户端即编译通过并跑通三形态

### Tests for User Story 4 ⚠️

- [x] T020 [US4] 新建 `libs/api/test/test_streaming_codegen.cc`：生成
  头契约断言（编译期 + 金样片段）——IStreamService 三形态签名、
  StreamServiceProxy 返回 unique_ptr<ClientReader/Writer/ReaderWriter>、
  RegisterService 按 form 分派、未重写默认 = UNIMPLEMENTED 兜底；
  003 一元生成回归（既有 test_service_codegen.cc 全部保持通过）

### Implementation for User Story 4

- [x] T021 [US4] `generator/urpc_codegen.cc`：EmitInterface/EmitProxy/
  EmitRegister 按 kForm 展开（签名 = contracts §2/§3 逐字）；catch
  块中不得引用被 move 的 done（003 教训，对 流式适配器同样适用：
  `Finish(INTERNAL)` 路径持自身引用）
- [x] T022 [US4] `examples/streaming/streaming_server_main.*`：从手写
  注册切换为「继承 IStreamService + RegisterService」（删除手写适配
  用法）；client 切换为 StreamServiceProxy；全部 e2e 保持绿

**Checkpoint**: 类型化流式接口与一元统一为单一编程模型（US4 验收）

---

## Phase 7: User Story 5 - 与官方 gRPC 流式互通 (Priority: P2)

**Goal**: 官方 gRPC Python 对端双向验收三形态（宪法 I 发布门禁）

**Independent Test**: `ctest --preset interop` 流式子象限全 ok

- [x] T023 [US5] `interop/python/peer_client.py`：扩展官方→urpc 象限
  ——Range 按序收 5 条断言、Sum 上行 10 条断言合计 45、Chat 交错 5
  轮双向序列断言（沿用既有 exit-code 约定）
- [x] T024 [US5] `interop/python/peer_server.py`：官方服务端实现同名
  三方法；`examples/streaming/streaming_client_main.*` 增加对官方
  对端的三形态驱动模式
- [x] T025 [US5] `interop/python/run_interop.py`：接入流式子象限
  （A/B 双向；保留 grpcio 缺失 SKIP 语义）；本地有 grpcio 时
  `ctest --preset interop` 实测通过

**Checkpoint**: 三形态双向互通验证通过（SC-002）

---

## Phase 8: User Story 6 - 取消、截止时间与流控语义 (Priority: P2)

**Goal**: 取消/超时/背压/优雅关闭对三形态生效且与 gRPC 对齐（US6 全
部场景）

**Independent Test**: 取消、超时、慢消费三类场景测试独立运行通过

- [x] T026 [US6] `libs/api/test/test_streaming.cc` 取消用例：客户端
  中途 Cancel → 客户端得到取消终态、服务端 Write 返回 false 且
  OnCancel 观察到（US6 场景 1）、双向流取消后双方资源释放
- [x] T027 [US6] 截止时间用例：流中途到期 → DEADLINE_EXCEEDED、双方
  资源释放（FR-010）；超时传播（grpc-timeout 头）对整流生效
- [x] T028 [US6] 背压/内存用例：慢消费者（读侧人为滞后）+ 10,000 条
  消息：发送队列深度始终 ≤2、消息零丢失零重复、传输完成（FR-008/
  SC-004）
- [x] T029 [US6] 优雅关闭用例：在途流存在时 Shutdown → 停收新调用、
  宽限期内等待 Finish、超时强制 UNAVAILABLE（FR-016）；修复实现中
  暴露的问题

**Checkpoint**: US6 全场景绿；此阶段产生的修复不得破坏 US1–US5

---

## Phase 9: User Story 7 - 流式示例与性能基线 (Priority: P3)

- [x] T030 [P] [US7] 新建 `libs/api/bench/bench_streaming.cc`：
  BM_ServerStreamingThroughput（单流 10,000×1KB，msg/s）+
  BM_BidiRoundTrip（1KB 往返，μs）；`libs/api/CMakeLists.txt` 接入
  LABELS bench（沿用 bench_unary 管线，JSON 输出留档）
- [x] T031 [US7] `examples/streaming/README.md`：三形态 30 分钟上手
  说明（对照 SC-005）；核对 quickstart.md §2 输出样例与实现一致

---

## Phase 10: Polish & Cross-Cutting

- [x] T032 按 `specs/004-streaming-rpc/quickstart.md` §1–7 完整走查
  （构建回归 / 示例 / 互操作 / 基准 / 生成器契约 / SC 对照），逐节
  记录结果
- [x] T033 [P] 交叉核对 FR-001..016 与测试映射表（data-model.md 验证
  规则汇总），补漏；确认零回归（既有测试零改动零跳过）

---

## Dependencies & Execution Order

### Phase Dependencies

- Setup（Phase 1）→ Foundational（Phase 2）阻塞全部故事
- US1（Phase 3）与 US2（Phase 4）相互独立，可并行（分别以服务端写
  路径 / 客户端写路径为主，文件冲突面小：stream.h 需串行追加）
- US3（Phase 5）依赖 T008/T013 的合成原语；US4（Phase 6）依赖
  US1–US3 的类型形状；US5（Phase 7）依赖 US1–US3（示例三方法齐备，
  不依赖 US4）；US6（Phase 8）依赖 US1–US3；US7（Phase 9）依赖
  US1/US3（基准对象）与 Phase 8（背压结论）
- Polish（Phase 10）最后

### 并行机会

- T001/T002 可并行（不同文件）
- T003/T004/T005 串行为宜（同一批 core 文件；T004↔T005 文件不相交，
  谨慎可并行）
- US1 与 US2 由两人并行：A 做 T007–T011，B 做 T012–T015
  （stream.h/urpc_api_impl.cc 追加式编辑，注意合并顺序）
- T023/T024 可并行；T026–T029 串行（同一测试文件）

### Key Files

| 层 | 文件 | 涉及任务 |
|----|------|----------|
| core | router.h / urpc_router.cpp | T003 |
| core | server.h(StreamCallCtx) / urpc_server.cc | T004 |
| core | channel.h / urpc_channel.cc / h2_session.* | T005 |
| core test | test_streaming.cc | T006 |
| api | stream.h / detail/raw.h / urpc_api_impl.cc | T008–T014, T017–T018 |
| api test | test_streaming.cc / test_streaming_codegen.cc | T007, T012, T016, T020, T026–T029 |
| generator | urpc_codegen.cc | T002, T021 |
| examples | examples/streaming/** | T001, T010, T015, T019, T022, T024, T031 |
| interop | interop/python/** | T023–T025 |
| bench | libs/api/bench/bench_streaming.cc | T030 |

## Implementation Strategy

- **MVP First**：Phase 1→2→3（US1 服务端流）即可独立演示与验收；
  建议在此设置第一个外部检查点
- **Incremental**：+US2 → +US3（能力齐）→ +US4（类型化统一）→
  +US5（互通门禁）→ +US6（语义完备）→ +US7（基线留档）
- 每个故事完成即跑「全量既有测试零回归 + 新增用例绿」双门禁后再
  进入下一故事（宪法 IV；003 教训：新路径必须先有测试背书）
