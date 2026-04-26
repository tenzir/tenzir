Good. Now I can see the full picture. Here's the proposal:

`OperatorOutputBase<Output>` already has `process_sub()` which is explicitly
documented as callable **in parallel** with other methods and receives
`Push<Output>&`. So `Push<T>` is already proven thread-safe for concurrent use.
The precedent exists.

The minimal change is **one new virtual method** on `OperatorOutputBase<Output>`:

```cpp
/// Long-running concurrent task with access to Push<Output>.
///
/// Runs concurrently with process(), process_task(), and all other
/// operator methods — similar to await_task(), but with the ability
/// to push output directly.
///
/// The executor starts this task after start() and cancels it during
/// shutdown. The default sleeps forever (no concurrent output).
///
/// This enables operators that need to accept input and emit output
/// at the same time (e.g., a decoupling buffer).
virtual auto output_task(Push<Output>& push, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(push, ctx);
    co_await wait_forever();
}
```

Here's how this solves the buffer problem:

```
upstream → process() ──writes──→ [buffer] ←──reads── output_task() → push → downstream
              ↑                                           ↑
              └─ blocks on space_available_  ←── notify ──┘  (concurrent, no deadlock)
```

- **`process()`**: enqueues input into the buffer. When full, blocks on
  `space_available_` — safe because `output_task()` runs concurrently and will
  free space.
- **`output_task()`**: dequeues from the buffer, pushes downstream via
  `Push<T>`. When the buffer empties, waits on `data_available_`. When push blocks
  (slow downstream), it just waits — process() can still run concurrently and fill
  the buffer.
- **`finalize()`**: sets `closed`, notifies `output_task()`. Can either return
  `continue_` (let output_task drain) or drain itself and return `done`.

All four requirements are met:
- **(R1)** process() and output_task() are concurrent → upstream runs while downstream is slow ✓
- **(R2)** output_task() actively drains the buffer ✓
- **(R3)** Single bounded buffer, no secondary queues ✓
- **(R4)** finalize signals close, output_task drains and returns ✓

The change is **fully backward compatible**: the default sleeps forever, so
existing operators are unaffected. The executor change is also minimal — start
`output_task()` as a concurrent coroutine alongside `await_task()`, and cancel
it on shutdown (same lifecycle management that already exists for `await_task`).
