# High-Performance Graph Pattern Matching on TuGraph Analytics

**English** | [中文](README.zh-CN.md)

![Rank](https://img.shields.io/badge/rank-🥇_Champion-gold)
![Contest](https://img.shields.io/badge/contest-11th%20CCF%20BDCI%202023-blue)
![Language](https://img.shields.io/badge/language-Java-orange)

Champion solution of the [11th CCF Big Data & Computational Intelligence Contest — High-Performance Graph Pattern Matching Algorithm Design on TuGraph Analytics](https://www.datafountain.cn/competitions/975).

The task: given an LDBC Finbench dataset, answer four families of complex graph
pattern queries as fast as possible. This solution completes **all cases in
~6 seconds end-to-end on a MacBook Pro M2** (9–11s on a 16-core Linux server),
using only standard graph-computation APIs.

## Key optimizations

All of the below stay within the standard TuGraph Analytics (GeaFlow) API — no
cross-node data joins, vertex/edge data association, or edge merging tricks.

1. **Row-number-as-ID int encoding** — inspired by auto-increment IDs in RDBMSes,
   the hidden "line number" of input files replaces the original `long` IDs, with
   distinct offsets per node type so a plain `int` addresses billions of unique
   nodes. This significantly accelerates both graph construction and iteration.
2. **Hand-tuned graph diffusion algorithm** — a carefully designed diffusion
   computes all four cases for every node within 5 iterations; the whole compute
   phase finishes in under 2 seconds, outperforming brute-force traversal of
   in-memory collections. See the `CaseKiller` code comments for details.
3. **Custom Kryo serializers** for vertex / edge / message structs bring ~20%
   overall speedup in the construction and compute phases (`PVertex` / `PEdge` / `MValue`).
4. **Parallel & pre-allocated file I/O** — files are pre-read and pre-created,
   then read/written with multiple threads (`readAllData()` / `writeFiles()`).
5. **JVM tuning** — 8 worker threads (empirically optimal), ParallelGC instead
   of G1 with a larger young generation / Eden space for higher allocation throughput.

## Phase-by-phase breakdown (sf1 dataset)

| Phase | Time |
|---|---:|
| Total | **6055 ms** |
| Cluster startup, PipelineTask begins | 830 ms |
| File reading (vertexSource / edgeSource ready) | 30 ms |
| Graph construction | 2950 ms |
| Iteration 1 / 2 / 3 / 4 / 5 | 950 / 470 / 135 / 95 / 130 ms |
| Sink & result return | 210 ms |
| Node sorting & file writing | 230 ms |
| Final write, process exit | 5 ms |

## Reproduce

Standard Maven project:

```bash
mvn clean package
# place the LDBC Finbench dataset, then launch via the TuGraph Analytics
# pipeline entry in src/ (see code comments for stage-level timing switches)
```

The full optimization write-up (with per-stage methodology) is in
[README.zh-CN.md](README.zh-CN.md).
