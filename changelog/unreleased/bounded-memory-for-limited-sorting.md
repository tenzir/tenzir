---
title: Bounded memory for limited sorting
type: change
authors:
  - mavam
created: 2026-10-01T15:15:51.994413Z
---

Sorting with a downstream `head N` now uses memory proportional to the requested result size rather than the full input. For example, `sort -timestamp | head 10` still inspects every input event, but only retains the events needed for the result.

The same improvement applies to `top` and `rare` followed by `head`. Aggregation still processes the complete input and retains every group. Ordering, null placement, and filtering behavior are unchanged.

### Local CLI benchmarks

Selecting 10 events from two million shuffled events was about 4.2 times faster and used 65% less peak process memory than a full-sort control in a local benchmark:

| Input events | Input order | Result size | Full-sort runtime | Limited-sort runtime | Peak memory, full sort | Peak memory, limited sort |
| --- | --- | --- | --- | --- | --- | --- |
| 250,000 | Shuffled | 10 | 1.28 s | 0.97 s | 275 MiB | 222 MiB |
| 2,000,000 | Shuffled | 10 | 4.94 s | 1.17 s | 722 MiB | 254 MiB |
| 2,000,000 | Shuffled | 10,000 | 4.77 s | 1.18 s | 720 MiB | 304 MiB |
| 2,000,000 | Shuffled | 1,000,000 | 5.08 s | 5.38 s | 719 MiB | 775 MiB |
| 2,000,000 | Descending | 10 | 1.79 s | 1.25 s | 729 MiB | 330 MiB |

Measurements used an Apple M1 Ultra on macOS, an optimized build with assertions enabled, and disabled pipeline parallelism. Each event had a unique integer key and a 128-byte string payload. Shuffled inputs used a fixed random seed of 42. Input came from local BITZ files, and timed runs discarded the output. Values are medians of five sequential runs after a warmup. Runtime includes CLI startup and file reading; peak resident memory covers the entire CLI process, not just sorting.

The limited-sort pipeline used `sort key | head N | assert true | discard`. The full-sort control used `sort key | assert true | head N | discard`: the always-true assertion prevents limited sorting without changing the selected events. Separate checks confirmed identical ordered output for every scenario.

Small result sets benefited most in this workload. Selecting half the input had no clear runtime benefit and used about 8% more peak process memory than the full-sort control. Results depend on the workload and hardware.
