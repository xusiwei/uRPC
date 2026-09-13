# Quickstart: Streaming RPC（流式调用验证指南）

端到端验证清单：证明三种流式形态在「单元/集成、端到端示例、官方
gRPC 互操作、基准」四层全部可用。前置条件与构建说明与
[003 quickstart](../003-typed-service-interface/quickstart.md) 相同。

## 1. 构建与全量回归（FR-015 / SC-006）

```bash
cmake --preset release && cmake --build --preset release
ctest --preset verify --timeout 300        # 单元/集成/e2e 全套
```

预期：`100% tests passed`（含既有全部测试——一元与类型化接口零回归；
新增 `urpc-streaming` 套件：三形态 e2e）。

## 2. 手工体验：三形态端到端示例（US1/US2/US3）

```bash
# 终端 1：流式示例服务端（监听 127.0.0.1:50052）
./build/release/examples/streaming/urpc_streaming_server

# 终端 2：流式示例客户端（Range / Sum / Chat 各跑一轮）
./build/release/examples/streaming/urpc_streaming_client 127.0.0.1:50052
```

预期输出（节选）：

```text
[Range ] requested N=5, received 5 values in order: 0 1 2 3 4
[Sum   ] sent 10 values, total = 45
[Chat  ] 5 rounds, server echoed every message in order
```

`Range` 同时验证「按序投递」；`Sum` 验证「半关闭后服务端聚合」；
`Chat` 验证「写下行与读上行并行、双方独立收尾」（US3 场景 2）。

## 3. 三层自动化验证的内容与位置（FR-014）

| 层 | 位置 | 覆盖 |
|----|------|------|
| 组件单元 | `libs/core/test/test_streaming.cc`、`libs/api/test/test_streaming.cc` | 状态机不变式（Finish 恰一次/终态后写拒绝）、有界发送队列、分帧违规 |
| 端到端 | `examples/streaming/` + `ctest`（examples-streaming-e2e） | 三形态真机回环：顺序、eos、零消息流、中途取消、超时 |
| 互操作 | `ctest --preset interop`（需 grpcio；缺失自动 SKIP） | 官方 gRPC 双向三形态（见 §4） |

## 4. 与官方 gRPC 流式互通（FR-013 / SC-002）

```bash
python3 -m pip install --user -r interop/python/requirements.txt
ctest --preset interop --timeout 300
```

预期：既有两个象限 + 流式子象限全部 `ok`：

- A（官方客户端 → urpc 服务端）：`Range` 按序收 5 条、`Sum` 上行 10 条
  取合计、`Chat` 交错 5 轮。
- B（urpc 客户端 → 官方服务端，`peer_server.py`）：同一组三形态。

任一子象限失败即互操作门禁失败（宪法 I）。

## 5. 流式基准（FR-014 / SC-003/SC-004）

```bash
ctest --preset bench --timeout 600        # 含 bench_streaming
```

- `BM_ServerStreamingThroughput`：单流 1 万条 × 1KB，吞吐 msg/s。
- `BM_BidiRoundTrip`：双向流 1KB 往返，时延 μs。

预期：同机连续多轮波动 ≤30%；基准 JSON 输出到
`build/release/bench-streaming-last.json` 留档（沿用一元基准管线）。

## 6. 生成器契约抽查（FR-012 / US4）

- 对 `examples/streaming/streaming.proto` 的生成头检查：三种形态的
  接口/代理签名与 [contracts](./contracts/streaming-interface.md)
  §2–3 一致（编译期断言，见测试）。
- 用未改动的 003 时代 proto（一元）重新生成：既有用户代码（003 的
  测试与示例原样）编译通过——`kMethods` 三列扩展对二元初始化向后
  兼容。

## 7. 成功标准对照

| 标准 | 验证手段 |
|------|----------|
| SC-001 千条消息零丢失/顺序一致 | `Sum`/`Range` e2e + 单元按序断言（N=1000 用例） |
| SC-002 官方互通 | §4 |
| SC-003 基准波动 ≤30% | §5 |
| SC-004 内存有界 | 慢消费者用例（读侧人为滞后）+ 发送队列深度断言 |
| SC-005 30 分钟上手 | §2 示例 + 生成接口签名（无胶水代码） |
| SC-006 三平台 CI 全绿 | 推送后 GitHub Actions 矩阵 |
