# Implementation Plan: Streaming RPC（流式调用：服务端流 / 客户端流 / 双向流）

**Branch**: `004-streaming-rpc` | **Date**: 2026-09-13 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/004-streaming-rpc/spec.md`

## Summary

在既有 gRPC 兼容内核（libuv + nghttp2 + upb，三层 core/cabi/api）之上支持
其余三种调用形态：服务端流、客户端流、双向流，一元形态保持不变。核心思路：

1. **内核层**扩展统一的多消息调用模型：`ServerCallCtx` 派生
   `StreamCallCtx`（逐消息读、逐消息写、半关闭、Finish），路由按方法
   形态分发；响应侧复用既有 SendData/SendTrailers 原语，请求侧为
   HTTP/2 流增加「先开流、多次 DATA、END_STREAM 半关闭」的发送路径。
2. **API 层**提供类型化 Reader/Writer（同步阻塞式，沿用「禁止在事件
   循环线程内使用同步接口」的既定语义），复用一元的 Result/arena 所
   有权模型。
3. **生成器**识别 `stream` 关键字，按四种形态生成服务端接口方法与客户
   端代理入口，一元生成物字节级向后兼容（003 契约不变）。
4. 互操作（官方 gRPC Python 双向三形态）、三层测试、流式基准随特性
   交付（宪法 I/IV）。

## Technical Context

**Language/Version**: C++17（宪法约束，不容更改）

**Primary Dependencies**: libuv 1.48.0、nghttp2 1.65.0、upb（protobuf
30.2 内置）、GoogleTest 1.15.2、Google Benchmark 1.9.1（全部 vendored，
宪法 V 固定边界；本特性零新增依赖）

**Storage**: N/A（RPC 框架，无持久化）

**Testing**: GoogleTest（单元/集成/e2e）+ 官方 gRPC Python 互操作脚本
（interop/，沿用 001 决策）+ Google Benchmark（流式基准）

**Target Platform**: Linux、macOS、Windows（三平台 CI 全绿为合入门禁）

**Project Type**: C++ 库（三层：core 内核 / cabi 绑定边界 / api 类型化
C++），附 protoc 插件（generator/）与示例（examples/）

**Performance Goals**: 流式基准纳入既有 Google Benchmark 体系，同机多轮
波动 ≤30%（SC-003）；单流 10,000 条消息传输期间内存有界（SC-004）

**Constraints**: gRPC 线协议兼容不可妥协（宪法 I）：长度前缀帧序列、
END_STREAM 半关闭、trailers 结束状态、gRPC 状态码；禁止私有协议扩展；
一元与类型化接口零回归（FR-012/FR-015）

**Scale/Scope**: core/api 两层扩展 + 生成器 + 示例 + 互操作脚本 + 基准；
不触及 cabi 绑定边界的新增面（流式 C-ABI 留待多语言绑定阶段）

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| 原则 | 状态 | 依据 |
|------|------|------|
| I. gRPC 线协议兼容（不可妥协） | ✅ | 流式线上语义严格按 gRPC：同流按序长度前缀帧序列、END_STREAM 半关闭、trailers 终态、状态码；互操作三形态双向验收（FR-006/FR-013） |
| II. 分层内核与可绑定 API 边界 | ✅ | 多消息机制全部落在 core（libuv/nghttp2/upb 之上），类型化 Reader/Writer 为 api 层薄封装；cabi 边界本特性不扩展（演进空间保留，符合「评估不实现」要求） |
| III. 跨平台构建纪律 | ✅ | 全部平台相关代码沿用既有 platform 抽象；零新增条件编译；三平台 CI 门禁 |
| IV. 测试背书的变更（不可妥协） | ✅ | 每个形态配单元 + e2e + 互操作；流式基准入 Google Benchmark 体系；一元回归零容忍 |
| V. 依赖最小化且边界固定 | ✅ | 零新增依赖；互操作沿用官方 gRPC Python（既有开发依赖，不进运行时） |

**Phase 1 复核**：设计未引入越界复杂度（见 Complexity Tracking——无违例）。

## Project Structure

### Documentation (this feature)

```text
specs/004-streaming-rpc/
├── plan.md              # 本文件
├── research.md          # Phase 0：设计决策（8 项）
├── data-model.md        # Phase 1：实体/状态机/验证规则
├── contracts/
│   └── streaming-interface.md   # 生成接口/代理/内核原语契约
├── quickstart.md        # Phase 1：端到端验证指南
└── tasks.md             # Phase 2（/speckit.tasks 产出）
```

### Source Code (repository root)

```text
libs/core/                        # 内核层：多消息调用机制
├── include/urpc/core/
│   ├── router.h                  # + RegisterStream / MethodForm / StreamHandler
│   ├── server.h                  # ServerCallCtx 不变；+ StreamCallCtx 声明
│   ├── channel.h                 # + 流式调用原语（OpenStream/StreamSend/...）
│   └── codec.h                   # FrameDecoder 复用（多消息已支持）
└── source/
    ├── urpc_server.cc            # 流式分发、逐消息投递、Finish/取消/排空
    ├── urpc_channel.cc           # 流式发送路径（开流/多次 DATA/半关闭）
    └── urpc_h2_session.cc        # + SubmitHeadersOpen（非终态请求头）

libs/api/                         # 类型化 C++ API
├── include/urpc/
│   ├── stream.h                  # ServerReader/Writer/ReaderWriter、
│   │                             #   ClientReader/Writer/ReaderWriter（新）
│   ├── server.h                  # + RegisterStreamFor<M>
│   └── detail/raw.h              # + 流式 detail 桥接声明
├── source/urpc_api_impl.cc       # 流式桥接实现
└── test/test_streaming.cc        # 三形态 e2e（GoogleTest）

generator/urpc_codegen.cc         # 按形态生成接口/代理/注册（撤除 v1 拒绝）
examples/streaming/               # 新示例：三形态 server + client
interop/python/                   # peer_client/peer_server/run_interop 扩展流式象限
libs/*/bench/                     # bench_streaming（Google Benchmark）
```

**Structure Decision**: 沿用既有三层布局与 002/003 确立的目录纪律，不做
结构性调整；新增文件收敛为 `libs/api/include/urpc/stream.h`（类型化流式
面）与 `examples/streaming/`（示例），内核扩展全部内联进既有文件。

## Complexity Tracking

> 无宪法违例，本表留空。

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| （无） | | |
