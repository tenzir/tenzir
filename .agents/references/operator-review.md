# Operator review

Review TQL operators for correctness, resource handling, and executor behavior.

## General

- Does the operator use existing operator patterns for the same problem?
- Are blocking calls moved to `spawn_blocking(...)` or the correct external
  executor?
- Is backpressure bounded and explicit?
- Is snapshot state complete and bounded?
- Are argument validation errors located at the right source span?

## Shutdown behavior

- What happens when the source reaches end-of-stream?
- What happens when downstream completes early?
- What happens on cancellation, such as CTRL-C?
- Which object owns each resource, and where is it released?

## Source operators

- In high-traffic mode, are buffers bounded?
- In low-traffic mode, can a single event wait unnecessarily long before
  delivery?
- Are external callbacks or helper tasks coordinated through queues or channels
  rather than mutating operator state directly?

## Sink operators

For sink operators, `prepare_snapshot()` must return only after all pending
events are processed. This ensures correctness when the snapshot flows upstream.
