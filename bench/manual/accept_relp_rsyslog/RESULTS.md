# Recorded results

Recorded on August 17, 2026 with:

- Tenzir built in release mode.
- An Apple M1 Ultra Mac Studio with 20 CPU cores and 128 GB of memory.
- Docker Desktop 29.7.2.
- A native Arm64 Debian Bookworm container.
- rsyslog 8.2302 and librelp 1.11.0.
- One million repeated 135-byte RFC 5424 messages per run.

## Response-coalescing baseline

Tenzir commit `6cb1b42a` used a bounded per-connection response writer to
preserve transaction order while combining responses that arrived within 100
µs. The raw variant reached 90.5k events/s.

The equivalent Python RELP receiver sustained 136.6k events/s through the same
rsyslog container and Docker boundary. Profiling showed that bounded-queue and
executor handoffs, response entries, allocations, and coroutine resumptions
had become the main costs.

## Ingress and acknowledgement batching

Tenzir commit `4d5b1575` groups up to 16 payloads before crossing into the
operator executor and represents their ordered acknowledgements as one compact
range. The queue holds at most 64 such batches, preserving the previous
1,024-event bound. A short 100 µs response interval then combines ranges into
transport writes.

The one-million-event raw variant produced:

| Run | Seconds | Events/s | Payload MB/s | User CPU s | System CPU s | Peak RSS MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2.380 | 420,083.4 | 57.131 | 2.75 | 1.31 | 142.5 |
| 2 | 2.287 | 437,239.4 | 59.465 | 2.70 | 1.23 | 143.0 |
| 3 | 2.329 | 429,373.3 | 58.395 | 2.77 | 1.29 | 143.3 |
| **Median** | **2.329** | **429,373.3** | **58.395** | **2.75** | **1.29** | **143.0** |

A diagnostic raw run observed exactly 62,500 operator handoffs, or 16 events
per handoff, and 5,937 response writes, or 168 acknowledgements per write. It
reported no queue stalls.

The parsed variant, which includes Syslog and Grok parsing, reached a median
135.4k events/s. Parsing is therefore the next bottleneck after the RELP
transport path.

A 64 KiB transport buffer showed no measurable improvement over 16 KiB. After
introducing ingress batches, a 100 µs response interval outperformed 250 µs and
1 ms: the compact response ranges already amortize writes, so longer delays
primarily hold back the client window.

Compared with ordered response coalescing alone, ingress and acknowledgement
batching improved the median raw rate by 4.7x.

The payload rate counts the generated Syslog input, including its newline. It
excludes TCP and RELP framing overhead. Peak RSS covers the Tenzir process, not
the rsyslog container.
