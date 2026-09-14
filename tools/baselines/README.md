# Performance baselines (US7 / T044)

Baselines are recorded per platform with the pinned environment noted below.
The regression gate is `tools/compare_baseline.py` (>10% median regression
blocks, research.md §1).

Re-record after intentional performance-affecting changes:

```sh
./build/release/urpc/api/urpc_bench_unary --benchmark_min_time=0.3s \
    --benchmark_format=json --benchmark_out=tools/baselines/<platform>.json
```

Compare a fresh run against the baseline:

```sh
./build/release/urpc/api/urpc_bench_unary --benchmark_min_time=0.3s \
    --benchmark_format=json --benchmark_out=/tmp/bench.json
python3 tools/compare_baseline.py tools/baselines/<platform>.json /tmp/bench.json
```

## linux-x86_64 (2026-09-11)

- 4 vCPU @ 2.5GHz, Ubuntu 24.04, gcc 13.3, Release
- SerialRTT (1KiB payload): ~180 µs median (target p50 ≤ 300 µs — met)
- FourInFlightThroughput (4 in flight, 1KiB): baseline recorded as-is;
  sustained micro-batching shows jitter — optimization TODO for a later
  feature (correctness unaffected; SC-005 concurrency test green)

Note: `bench-unary-last.json` (build dir) holds the latest ctest run for
ad-hoc comparison; only files in this directory are the reviewed baseline.
