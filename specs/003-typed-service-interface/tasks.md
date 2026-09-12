# Tasks: 类 gRPC 的类型化服务接口（Typed Service Interface）

**Input**: Design documents from `/specs/003-typed-service-interface/`

**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/service-interface.md ✅, quickstart.md ✅

**Tests**: 必须包含（宪法原则 IV 不可妥协 + 规格 US4 全量回归要求；研究 §5 四层验证策略落地）。

**Organization**: 按用户故事分组。**依赖说明**：US1/US2/US3 同为 P1，但
US3（工具生成）是 US1/US2 的编译前提（生成物提供接口/代理类型），故
执行顺序 US3 → US1 ∥ US2 → US5 → US4。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行（不同文件、无未完成依赖）
- **[Story]**: 所属用户故事（US1..US5）
- 任务描述含精确文件路径

## Path Conventions

单仓库布局（plan.md Project Structure）：`generator/`、`cmake/`、
`libs/api/`、`examples/echo/`、`tools/`。规格契约引用：
`specs/003-typed-service-interface/contracts/service-interface.md`（下称
"契约"）。

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 生成器模块骨架接入构建，插件链路先打通

- [x] T001 创建生成器模块骨架：`generator/CMakeLists.txt` 定义可执行目标 `protoc-gen-urpc`（链接 vendored `libprotoc`/`libprotobuf`，参照 `third_party/protobuf` 中 protoc-gen-upb 目标的链接形态）；`generator/urpc_codegen.cc` 写入 protoc 插件 `main` + 一个按 `CodeGenerator` 接口注册的空生成器（当前对 `--urpc_out` 输出占位空文件即可）；在 `cmake/urpc-deps.cmake` 挂载该目标（与 `URPC_PROTOC_PLUGIN_UPB` 同体系，新增 `URPC_PROTOC_PLUGIN_URPC` 变量指向 `$<TARGET_FILE:protoc-gen-urpc>`）；本地 `cmake --build --preset release` 构建通过
- [x] T002 在 `cmake/urpc-proto.cmake` 的 `urpc_proto_upb()` 中把 `--plugin=protoc-gen-urpc=${URPC_PROTOC_PLUGIN_URPC}` 与 `--urpc_out=${ARG_OUT_DIR}` 追加进既有 `add_custom_command`（产物 `<file>.service.h`/`<file>.service.cc` 声明进 OUTPUT）；确认 examples/echo 的 upb 生成命令仍正常、占位 service 文件出现在 `${URPC_ECHO_GEN_DIR}`（构建产物验证）

**Checkpoint**: 插件在标准构建链中被调用（COMMENT 行出现 urpc codegen），三平台无平台分支。

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: API 层公共底座——三个新头文件，US1/US2/US3 的编译前提

**⚠️ CRITICAL**: US3 生成物直接 include 这些底座，本阶段必须先完成

- [x] T003 [P] 创建 `libs/api/include/urpc/service.h`：定义 `MethodDescriptor`（`name`/`path` 字符串视图对，见 data-model.md "MethodDescriptor" 表，字段约束：与 .proto 方法集一一对应、顺序稳定）；声明生成接口/注册辅助的公共约定（文档注释指向契约 §1/§2）；仅依赖既有公共头（`urpc/server.h`、`urpc/core/status.h`），无新依赖
- [x] T004 [P] 创建 `libs/api/include/urpc/proxy.h`：定义 `ProxyBase`——持有 `Channel*` 与可选 `Client*`（不可变引用、无业务状态，FR-006 线程安全由无状态保证）；提供模板辅助 `CallAsync<M>`/`Call<M>`（把现 `libs/api/include/urpc/client.h` 中 `Channel::CallAsync/Call` 的 upb 编解码内联逻辑抽取为底座可复用形式，语义不变：timeout_ms=0 不超时、事件循环线程内同步快速失败）
- [x] T005 [P] 扩展 `libs/api/include/urpc/client.h`：新增 `Client` 类（控制面：包装 `Channel` + `Options` 挂载点 + `template<class P> P Proxy()` 工厂），`Channel` 保持连接语义不变（target/Connect/关闭）；**存量 `Channel::Call/CallAsync` 原样保留**（FR-014）；`libs/api/source/urpc_api_impl.cpp` 相应最小实现，`libs/api/CMakeLists.txt` 无需新文件（头文件内联 + impl 扩展）

**Checkpoint**: `libs/api` 独立构建通过；三平台构建零告警新增；存量测试不动即绿。

---

## Phase 3: User Story 3 - 接口与代理由工具生成 (Priority: P1)

**Goal**: `--urpc_out` 从 .proto 确定性生成三件套（接口/代理/注册辅助），编译期即契约

**Independent Test**: 修改 `examples/echo/echo.proto` 新增方法 → 重新构建 → 业务类编译失败指向缺失重写（quickstart §3 / SC-002）

### Implementation for User Story 3

- [x] T006 [US3] 在 `generator/urpc_codegen.cc` 实现接口生成：遍历 `CodeGeneratorRequest` 的 FileDescriptorProto service/method；产出 `<file>.service.h` 中 `namespace urpc::gen::<proto_package>` 的 `I<Service>` 纯虚接口类——每方法一个 `virtual void <Method>(ServerContext&, const <Prefix>_<Req>*, UnaryDone<Prefix>_<Res>>)`（签名与 research.md §2 定案逐字一致），未重写默认实现体 `done(Status(kUnimplemented, "<Method> not implemented"), nullptr)`（FR-004）；`static constexpr MethodDescriptor kMethods[]`（name/path 与方法集一一对应，data-model 不变式）；upb C 符号（`<Prefix>_<Msg>` 与 minitable）从描述推导，与既有 `URPC_UNARY_METHOD` 的符号拼接规则一致（`<package>__<file>` 前缀）
- [x] T007 [US3] 在 `generator/urpc_codegen.cc` 实现代理与注册生成（同文件，接 T006）：`<file>.service.h` 内 `class <Service>Proxy : public I<Service>`——构造 `explicit <Service>Proxy(Channel*)` / `explicit <Service>Proxy(Client*)`，每方法 `<Method>Async(req, timeout_ms, done)` 覆写（转调 `ProxyBase::CallAsync`）与同步 `Result<Res> <Method>(req, timeout_ms)`（转调 `ProxyBase::Call`，契约 §1 形态）；`<file>.service.h` 尾部 free function `Status RegisterService(Server&, I<Service>&)`——按 kMethods 逐方法经 `detail::RegisterUnaryRaw` 等价通道注册 lambda（转调 `impl.<Method>(ctx, req, done)`，外层 try/catch 将异常转 `INTERNAL` 结束该次调用、进程存活，FR-013）；`<file>.service.cc` 收纳需要定义的符号
- [x] T008 [US3] 生成物构建接线收尾：确认 `examples/echo/CMakeLists.txt` 消费 `echo.service.h/.cc`（`urpc_proto_upb` 的 TARGETS 机制自动加源与 include）；echo 生成物在本地三步验证——① `cmake --build --preset release` 通过（含 include 生成头的编译 TU）② 连续两次构建生成物字节一致（确定性，quickstart §5）③ `grep` 生成物含 `class IEchoService`/`class EchoServiceProxy`/`RegisterService`
- [x] T009 [P] [US3] 新增 `libs/api/test/test_service_codegen.cpp`：include `echo.upb.h` 与 `echo.service.h`——① `static_assert` 方法表与方法集一致（数量 + 名称集合，SC-002 机制）② 金样片段断言：生成头包含接口纯虚签名、UNIMPLEMENTED 默认体、Proxy 继承关系、双构造等关键片段（研究 §5 金样内联策略）③ `EchoServiceProxy`/`RegisterService` 符号可寻址（链接级验证）；挂入 `libs/api/CMakeLists.txt` 测试目标

**Checkpoint**: US3 独立可验——proto 演进由编译器背书（quickstart §3 演示通过）。

---

## Phase 4: User Story 1 - 服务端以接口继承方式实现业务 (Priority: P1) 🎯 MVP 核心

**Goal**: 业务类继承 `IEchoService` 重写方法、实例整体注册到 Server，全方法路由正确

**Independent Test**: 用**存量客户端**（`Channel::Call<EchoMethod>`，FR-014 兼容面）驱动新式服务端，验证路由/兜底/拒绝/上下文（不依赖 US2）

### Tests for User Story 1（先写、先红）

- [x] T010 [US1] 在 `libs/api/test/test_typed_e2e.cpp` 写服务端故事测试（先失败）：① 继承 `IEchoService` 只重写 `Echo` → 存量客户端调 `Echo` 得正确响应、调 `SlowEcho` 得 UNIMPLEMENTED（FR-004）② `RegisterService(server, impl)` 后全方法可路由（FR-001/003）③ 同服务名二次注册返回明确错误（FR-003）④ ctx 取消通知/剩余时间在接口形态下可用（FR-005：复用 001 的 deadline 用例形态，客户端 deadline 300ms → 服务端 OnCancel 触发）⑤ 业务方法抛异常 → 客户端收 INTERNAL、后续调用正常（FR-013）

### Implementation for User Story 1

- [x] T011 [US1] 按 T010 红测实现/修正：`generator/urpc_codegen.cc` 的 RegisterService 发射体与 `libs/api/source/urpc_api_impl.cpp`/`service.h` 的桥接细节（lambda 捕获、异常守卫、方法路径 `/example.EchoService/Echo` 拼接与 Router 既有拒绝行为对齐）；修至全绿

**Checkpoint**: MVP 达成（Foundational + US3 + US1）——新式服务端 + 存量客户端即端到端可用。

---

## Phase 5: User Story 2 - 客户端以代理对象调用业务方法 (Priority: P1)

**Goal**: `EchoServiceProxy` 从 Channel/Client 构造，同步/异步/并发/deadline 全通

**Independent Test**: 新式客户端（代理）驱动**存量式服务端**（`RegisterUnaryFor` 注册，FR-014）完成调用（不依赖 US1 的测试实现）

### Tests for User Story 2（先写、先红）

- [x] T012 [US1][US2] 在 `libs/api/test/test_typed_e2e.cpp` 追加客户端故事测试（先失败）：① 代理同步+异步调用全方法往返正确（FR-006）② 不存在方法 → UNIMPLEMENTED、服务不可达 → UNAVAILABLE（US2-2）③ 多线程共享同一代理并发调用、结果正确配对（FR-006）④ `timeout_ms` 到期 → DEADLINE_EXCEEDED 且服务端可感知取消（FR-012/US2-4）⑤ 框架事件循环线程内同步调用 → 立即 INTERNAL（契约 §5）

### Implementation for User Story 2

- [x] T013 [US2] 按 T012 红测实现/修正：`libs/api/include/urpc/proxy.h` 的 ProxyBase 编解码/等待逻辑与 `generator/urpc_codegen.cc` 的代理发射体；修至全绿

**Checkpoint**: 双端新式闭环可用；US1+US2 可分别独立演示。

---

## Phase 6: User Story 5 - 控制面与业务面分离 (Priority: P2)

**Goal**: Channel=连接 / Client=控制面 / Proxy=业务面的职责边界经测试固化

**Independent Test**: Channel 关闭后代理调用立即失败；代理类型表面仅含业务成员（US5 验收场景）

### Tests for User Story 5（先写、先红）

- [x] T014 [US5] 在 `libs/api/test/test_typed_e2e.cpp` 追加职责分离测试（先失败）：① 构造 Channel→Client→Proxy 链，`Channel::Connect` 后销毁 Channel → 后续代理调用立即 UNAVAILABLE、不挂起不崩溃（FR-009）② 从 `Client::Proxy<EchoServiceProxy>()` 构造的代理与从 Channel 构造的行为一致（FR-010）③ 编译期表面审查：`static_assert` 代理类型无可访问的连接管理成员（用成员探测器模板断言无 `Connect`/`Close`/`Shutdown` 成员，US5-2 判据）④ Client 控制面选项（max_receive_message_size）生效路径明确（FR-010）

### Implementation for User Story 5

- [x] T015 [US5] 按 T014 红测修正 `libs/api/include/urpc/client.h` 的 Client/Channel 职责整理（若表面审查暴露越界成员则收敛访问性；**不得删除存量公共入口**，FR-014）；修至全绿

**Checkpoint**: 三分职责经编译期+运行时双重固化。

---

## Phase 7: User Story 4 - 示例迁移与全量回归 (Priority: P2)

**Goal**: echo 示例成为新接口活文档；全量测试+互操作全绿，零覆盖损失

**Independent Test**: 三平台构建运行迁移后示例与全量 ctest（quickstart §1/§2）

- [x] T016 [US4] 迁移 `examples/echo/echo_server_main.cpp`：`class EchoServiceImpl : public urpc::gen::example::IEchoService`（重写 Echo/SlowEcho，保留取消感知演示），`RegisterService(*server, impl)` 替换两处 `RegisterUnaryFor`；删除不再需要的 `URPC_UNARY_METHOD` 宏与手写编解码（SC-004 目标：用户代码 ≤ 现形态 70% 且零方法名字符串）
- [x] T017 [US4] 迁移 `examples/echo/echo_client_main.cpp`：`Channel::Connect` → `Client` → `EchoServiceProxy`，全部调用改走代理（含 `--expect-error` 模式的 UNIMPLEMENTED 断言路径）；`grep -c 'EchoService"'` 两文件为 0
- [x] T018 [US4] 全量回归与度量：本地 `ctest --preset verify` 全绿（含新增 codegen/e2e 测试与**存量 lambda 式测试原样通过**，FR-014）；`ctest --preset interop` gRPC Python 对端全绿（FR-012）；基准 `ctest --preset bench` 对照留档基线无 >10% 中位退化；记录 SC-004 行数对比结论于 PR 描述

**Checkpoint**: 规格 SC-001..005 全部可判定。

---

## Phase 8: Polish & Cross-Cutting Concerns

- [x] T019 [P] 更新 `README.md` 快速上手（服务端继承/代理调用三行示例）与 `examples/echo/` 头注释指向契约；`specs/003-typed-service-interface/` 工件交叉链接复核
- [x] T020 按 `specs/003-typed-service-interface/quickstart.md` §1–§6 全流程人工验证一遍（含 §3 接口演进演示、§5 确定性对比），记录结果
- [x] T021 宪法终检：PR/commit 说明声明触及原则条目（I: 线协议不变-互操作背书；IV: 新测试清单）；确认零新增依赖、零平台分支散布

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: 无依赖，立即开始（T001→T002 串行：目标先于接线）
- **Foundational (Phase 2)**: 依赖 Phase 1（生成物 include 底座前底座需存在——严格说 T003–T005 与 T002 无强序，但为叙事清晰先 Setup 后 Foundational；三任务彼此 [P]）
- **US3 (Phase 3)**: 依赖 Phase 1+2（插件链路 + 底座）；T006→T007→T008 串行（同文件递进），T009 [P] 可与 T008 并行（测试先行等待生成物落地后转绿）
- **US1 (Phase 4)**: 依赖 US3 完成；T010（测试）→ T011（实现）
- **US2 (Phase 5)**: 依赖 US3 完成；与 US1 **可并行**（不同验证面；T012 测试可先行——其中服务端搭建用存量注册式，绕开对 T011 的依赖）
- **US5 (Phase 6)**: 依赖 US2（代理存在才有分离可验）
- **US4 (Phase 7)**: 依赖 US1+US2+US5（迁移目标形态确定后）
- **Polish (Phase 8)**: 依赖全部故事完成

### User Story Dependencies

- **US3 (P1)**: Foundational 后即可——其余 P1 的编译前提
- **US1 (P1)**: US3 后；不依赖 US2（存量客户端驱动）
- **US2 (P1)**: US3 后；不依赖 US1（存量式服务端驱动）
- **US5 (P2)**: US2 后
- **US4 (P2)**: US1+US2+US5 后（终态集成）

### Parallel Opportunities

- Phase 2：T003/T004/T005 三个不同头文件，可三人并行
- Phase 3：T009（测试文件）与 T008（接线收尾）不同文件可并行
- Phase 4 ∥ Phase 5：US1 与 US2 不同测试面，可并行推进
- Phase 7：T016/T017 两个示例文件可并行
- Phase 8：T019 与 T020/T021 可并行

---

## Parallel Example: User Story 1 与 US2 并行

```text
Developer A（US1）: T010 服务端故事红测 → T011 桥接实现转绿
Developer B（US2）: T012 客户端故事红测（服务端用存量注册式）→ T013 ProxyBase/生成器修正转绿
汇合点: Phase 6 T014 职责分离测试同时覆盖两端的 Channel/Proxy 面
```

---

## Implementation Strategy

### MVP First（Foundational + US3 + US1）

1. Phase 1–2：插件链路 + 三底座（~1 天粒度）
2. Phase 3：生成器三件套 + 契约测试
3. Phase 4：服务端故事（存量客户端驱动验证）
4. **STOP and VALIDATE**：此时已是可用增量——新式服务端 + 任意客户端

### Incremental Delivery

1. MVP（如上）→ 演示
2. + US2 代理 → 双端新式闭环 → 演示
3. + US5 三分固化
4. + US4 示例迁移 + 全量回归 → 终态（SC 全判定）

---

## Notes

- 生成器输出必须确定性（同输入字节级一致）——quickstart §5 的验证依赖此性质
- 所有新生成/新增代码 C++17（宪法技术栈条目）；插件源码同标准
- 存量入口（`RegisterUnaryFor`/`Channel::Call*`）任何任务不得删除或改语义（FR-014）
- 每个任务或逻辑组完成后提交（commit 信息声明宪法触及条目，宪法开发流程条目）
- 避免：模糊任务、同文件并行冲突、跨故事依赖破坏独立性
