//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_builder.hpp"
#include "clickhouse/connection.hpp"
#include "clickhouse/easy_client.hpp"
#include "clickhouse/error_codes.h"
#include "clickhouse/exceptions.h"
#include "clickhouse/table_registry.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/request_window.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/concept/printable/tenzir/json2.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/view3.hpp"

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <fmt/format.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>

#include <memory>

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

namespace {

using namespace si_literals;

constexpr auto clickhouse_plaintext_port = uint64_t{9000};
constexpr auto clickhouse_tls_port = uint64_t{9440};
auto clickhouse_error_diagnostic(std::string_view message, location loc)
  -> diagnostic_builder {
  return diagnostic::error("ClickHouse error: {}", message).primary(loc);
}

auto clickhouse_openssl_error_diagnostic(std::string_view message, location loc,
                                         bool tls_enabled)
  -> diagnostic_builder {
  return add_tls_client_diagnostic_hints(
    clickhouse_error_diagnostic(message, loc), tls_enabled, "ClickHouse",
    clickhouse_plaintext_port, clickhouse_tls_port);
}

struct ToClickhouseArgs {
  located<secret> uri = {secret::make_literal(""), location::unknown};
  located<secret> host = {secret::make_literal("localhost"), location::unknown};
  Option<located<uint64_t>> port = None{};
  located<secret> user = {secret::make_literal("default"), location::unknown};
  located<secret> password = {secret::make_literal(""), location::unknown};
  ast::expression table = {};
  located<std::string> mode = {
    std::string{to_string(clickhouse::mode::create_append)}, location::unknown};
  Option<ast::field_path> primary = None{};
  Option<located<data>> tls;
  location operator_location;
  uint64_t jobs = 1;
  // Internal per-schema batching: rows accumulate per schema and are only sent
  // once one of these thresholds is hit. This coalesces the tiny per-schema
  // slices that heterogeneous input (e.g. OCSF) produces into larger inserts.
  uint64_t max_batch_rows = 64_Ki;
  duration batch_timeout = std::chrono::seconds{1};
  // A single field or list of fields to create as `JSON` columns. Unset when
  // `kind` is null.
  ast::expression json = {};
  // A single field or list of fields to create as `LowCardinality(<inner>)`
  // columns. Unset when `kind` is null.
  ast::expression low_cardinality = {};
};

class ToClickhouse final : public Operator<table_slice, void> {
public:
  /// Workers pull resolved table names off this queue; `None` is the shutdown
  /// signal (one per worker).
  using WorkQueue = folly::coro::BoundedQueue<Option<std::string>>;

  /// Per-table accumulation buffer. Slices resolved to the same target table
  /// are collected here (possibly across many Tenzir schemas) until a size or
  /// timeout threshold hands the table to a worker.
  struct table_buffer {
    std::vector<table_slice> events;
    uint64_t num_buffered = {};
    /// When this table first became non-empty while not enqueued; drives the
    /// timeout flush.
    std::chrono::steady_clock::time_point added
      = std::chrono::steady_clock::now();
    /// True while a worker holds this table's work (so `process`/timeout do not
    /// enqueue it again; new rows accumulate for the next round).
    bool enqueued = false;
  };

  /// State that must be mutated together under the async `Mutex`.
  struct locked_state {
    /// Per-resolved-table accumulation buffers.
    std::unordered_map<std::string, table_buffer> buffers;
    /// Number of slice-batches a worker has taken but not yet finished
    /// inserting. Together with empty `buffers` this means "fully drained".
    uint64_t in_flight = 0;
  };

  /// State shared between the operator sequence (producer) and the worker tasks
  /// (consumer). The multi-step operations that must keep the guarded data, the
  /// atomics, the notifies, and the queue consistent are member functions so
  /// they live in one place; the simple members (queue, counters, done, worker
  /// handles) are used directly.
  struct runtime_state {
    runtime_state(uint32_t queue_capacity, uint64_t max_batch_rows,
                  duration batch_timeout, uint64_t capacity_rows)
      : queue{queue_capacity},
        max_batch_rows{max_batch_rows},
        batch_timeout{batch_timeout},
        capacity_rows{capacity_rows} {
    }

    Mutex<locked_state> locked{locked_state{}};
    /// Total rows currently held in memory (buffered + taken-but-not-inserted).
    /// Atomic so `wait_for_capacity` can read it without the lock; the soft
    /// bound tolerates a slightly stale read and re-checks after each wake.
    Atomic<uint64_t> pending_rows{0};
    /// Woken when a table finishes inserting or a worker drains a buffer, so
    /// `wait_until_drained` can re-check.
    Notify drained;
    /// Woken when the set of pending timeouts may have changed (a buffer was
    /// added), so `await_task` recomputes its deadline.
    Notify buffer_ready;
    /// Woken when a worker frees rows, so `wait_for_capacity` can resume.
    Notify space_available;
    /// Distributes resolved table names to workers; `None` is the shutdown
    /// sentinel (one per worker).
    WorkQueue queue;
    Atomic<bool> done{false};
    MetricsCounter bytes_write_counter;
    MetricsCounter events_write_counter;
    std::vector<AsyncHandle<void>> worker_handles;
    /// Flush a table's buffer once it reaches this many rows.
    uint64_t max_batch_rows;
    /// Flush a table's buffer this long after its first buffered row.
    duration batch_timeout;
    /// Block `process` once the in-memory buffer reaches this many rows.
    uint64_t capacity_rows;

    /// A batch of slices taken from one table's buffer, with pre-computed sizes.
    struct taken {
      std::vector<table_slice> events;
      uint64_t rows = 0;
      uint64_t bytes = 0;
    };

    /// Buffers a batch of contiguous runs, each destined for a resolved table,
    /// enqueuing any table that crosses `max_batch_rows`. Wakes `await_task`.
    /// Takes the shared lock exactly once for the whole batch.
    auto buffer_all(std::vector<std::pair<std::string, table_slice>> runs)
      -> Task<void> {
      auto total_rows = uint64_t{0};
      auto to_enqueue = std::vector<std::string>{};
      {
        auto guard = co_await locked.lock();
        for (auto& [table, run] : runs) {
          const auto rows = run.rows();
          total_rows += rows;
          auto& entry = guard->buffers[table];
          if (entry.events.empty() and not entry.enqueued) {
            entry.added = std::chrono::steady_clock::now();
          }
          entry.num_buffered += rows;
          entry.events.push_back(std::move(run));
          if (entry.num_buffered >= max_batch_rows and not entry.enqueued) {
            entry.enqueued = true;
            to_enqueue.push_back(table);
          }
        }
      }
      pending_rows.fetch_add(total_rows, std::memory_order_relaxed);
      for (auto& table : to_enqueue) {
        co_await queue.enqueue(std::move(table));
      }
      buffer_ready.notify_one();
    }

    /// Blocks until the in-memory buffer drops back under `capacity_rows`,
    /// propagating backpressure upstream. Returns immediately once shutting
    /// down.
    ///
    /// This handles a theoretical deadlock with many buffered events but no
    /// work queued yet, where we would wait in `process`, but no timeout could
    /// happen because its in `process_task`. We solve this by triggering a full
    /// flush in this case. This is acceptable for this case, because it means a
    /// highly fragmented datastream and avoiding the deadlock takes priority
    /// over performance.
    auto wait_for_capacity() -> Task<void> {
      while (pending_rows.load(std::memory_order_acquire) >= capacity_rows
             and not done.load(std::memory_order_acquire)) {
        if (co_await in_flight_count() == 0) {
          co_await enqueue_matching([](const table_buffer&) {
            return true;
          });
        }
        co_await space_available.wait();
      }
    }

    /// Enqueues every non-empty, not-yet-enqueued table for which `pred(entry)`
    /// holds (marks it enqueued under the lock, pushes the name outside it).
    template <class Predicate>
    auto enqueue_matching(Predicate pred) -> Task<void> {
      auto to_enqueue = std::vector<std::string>{};
      {
        auto guard = co_await locked.lock();
        for (auto& [name, entry] : guard->buffers) {
          if (not entry.enqueued and not entry.events.empty() and pred(entry)) {
            entry.enqueued = true;
            to_enqueue.push_back(name);
          }
        }
      }
      for (auto& name : to_enqueue) {
        co_await queue.enqueue(std::move(name));
      }
    }

    /// Reads the current in-flight insert count under the lock.
    auto in_flight_count() -> Task<uint64_t> {
      auto guard = co_await locked.lock();
      co_return guard->in_flight;
    }

    /// The earliest `added + batch_timeout` across pending buffers, or none.
    auto earliest_deadline()
      -> Task<Option<std::chrono::steady_clock::time_point>> {
      auto guard = co_await locked.lock();
      auto result = Option<std::chrono::steady_clock::time_point>{};
      for (const auto& [name, entry] : guard->buffers) {
        if (entry.enqueued or entry.events.empty()) {
          continue;
        }
        auto deadline = entry.added + batch_timeout;
        if (not result or deadline < *result) {
          result = deadline;
        }
      }
      co_return result;
    }

    /// Blocks until nothing is buffered and no insert is in flight.
    auto wait_until_drained() -> Task<void> {
      while (true) {
        {
          auto guard = co_await locked.lock();
          auto pending = guard->in_flight;
          for (const auto& [name, entry] : guard->buffers) {
            pending += entry.events.size();
          }
          if (pending == 0) {
            co_return;
          }
        }
        co_await drained.wait();
      }
    }

    /// Takes the entire buffer for `table` and marks it in-flight. Returns an
    /// empty batch when the buffer was already drained by another path (its
    /// `enqueued` flag is reset and drainage waiters are woken).
    auto take(const std::string& table) -> Task<taken> {
      auto result = taken{};
      {
        auto guard = co_await locked.lock();
        auto it = guard->buffers.find(table);
        if (it == guard->buffers.end() or it->second.events.empty()) {
          if (it != guard->buffers.end()) {
            it->second.enqueued = false;
          }
          guard.unlock();
          drained.notify_one();
          co_return result;
        }
        result.events = std::exchange(it->second.events, {});
        guard->buffers.erase(it);
        guard->in_flight += 1;
      }
      for (const auto& s : result.events) {
        result.rows += s.rows();
        result.bytes += s.approx_bytes();
      }
      co_return result;
    }

    /// Releases the accounting for a completed insert of `rows` rows. On
    /// failure sets `done` *before* waking waiters so a blocked producer
    /// observes the shutdown instead of re-waiting.
    auto finish_insert(uint64_t rows, bool ok) -> Task<void> {
      pending_rows.fetch_sub(rows, std::memory_order_release);
      {
        auto guard = co_await locked.lock();
        guard->in_flight -= 1;
      }
      if (not ok) {
        done.store(true, std::memory_order_release);
      }
      drained.notify_one();
      space_available.notify_one();
    }
  };

  explicit ToClickhouse(ToClickhouseArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    TENZIR_ASSERT(not state_);
    const auto queue_capacity = detail::narrow_cast<uint32_t>(args_.jobs * 2);
    // Bound the in-memory buffer so `process` applies backpressure upstream
    // instead of accumulating without limit. Sized so every worker can hold ~a
    // batch in-flight while another batch accumulates before we block.
    const auto capacity_rows = args_.max_batch_rows * (args_.jobs + 1);
    state_ = std::make_unique<runtime_state>(
      queue_capacity, args_.max_batch_rows, args_.batch_timeout, capacity_rows);
    auto& dh = ctx.dh();
    auto mode_val = from_string<enum clickhouse::mode>(args_.mode.inner);
    TENZIR_ASSERT(mode_val);
    auto primary = Option<located<std::string>>{};
    if (args_.primary) {
      // We know that primary is a top level field, as it was validated.
      primary = {args_.primary->path().front().id.name,
                 args_.primary->get_location()};
    }
    auto json_columns = std::vector<located<field_path_type>>{};
    if (args_.json.kind) {
      // The `json` argument was already validated during parsing, so
      // re-extracting the field paths here cannot fail. These only affect
      // table *creation* (the columns are created as `JSON`); the worker
      // discovers which columns are JSON from the actual table and serializes
      // accordingly.
      auto parsed = parse_json_field_argument(args_.json, dh);
      TENZIR_ASSERT(parsed);
      json_columns = std::move(*parsed);
    }
    auto low_cardinality_columns = std::vector<located<field_path_type>>{};
    if (args_.low_cardinality.kind) {
      // The `low_cardinality` argument was already validated during parsing, so
      // re-extracting the column names here cannot fail.
      auto parsed = parse_field_list_argument(args_.low_cardinality, dh,
                                              "low_cardinality");
      TENZIR_ASSERT(parsed);
      low_cardinality_columns = std::move(*parsed);
    }
    auto ssl_opts = tls_options::from_optional(args_.tls);
    auto ssl = ssl_opts.resolve(ctx.actor_system().config(), dh);
    if (not ssl) {
      state_->done.store(true, std::memory_order_release);
      co_return;
    }
    auto const tls_enabled = ssl->tls.inner;
    auto const default_port
      = tls_enabled ? clickhouse_tls_port : clickhouse_plaintext_port;
    auto client_args = easy_client::arguments{
      .host = "", // resolved as secret below.
      .port = args_.port ? *args_.port
                         : located<uint64_t>{default_port, location::unknown},
      .user = "default", // resolved as secret below.
      .password = "",    // resolved as secret below.
      .default_database = None{},
      .set_client_default_database = false,
      .ssl = std::move(*ssl),
      .table = args_.table,
      .mode = {*mode_val, args_.mode.source},
      .primary = std::move(primary),
      .json = std::move(json_columns),
      .low_cardinality = std::move(low_cardinality_columns),
      .operator_location = args_.operator_location,
    };
    auto uri = std::string{};
    auto requests = std::vector<secret_request>{};
    auto has_uri = args_.uri.inner != secret::make_literal("");
    if (has_uri) {
      requests.push_back(make_secret_request("uri", args_.uri, uri, dh));
    } else {
      requests.push_back(
        make_secret_request("host", args_.host, client_args.host, dh));
      requests.push_back(
        make_secret_request("user", args_.user, client_args.user, dh));
      requests.push_back(make_secret_request("password", args_.password,
                                             client_args.password, dh));
    }
    auto ok = co_await ctx.resolve_secrets(std::move(requests));
    if (not ok) {
      state_->done.store(true, std::memory_order_release);
      co_return;
    }
    state_->bytes_write_counter
      = ctx.make_counter(MetricsLabel{"operator", "to_clickhouse"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    state_->events_write_counter
      = ctx.make_counter(MetricsLabel{"operator", "to_clickhouse"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    if (has_uri) {
      auto parsed = parse_connection_uri(uri, args_.uri.source, dh);
      if (not parsed) {
        state_->done.store(true, std::memory_order_release);
        co_return;
      }
      apply_connection_uri(client_args, *parsed);
      if (not parsed->has_port()) {
        client_args.port = located<uint64_t>{default_port, location::unknown};
      }
    }
    for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
      // The worker and its ping loop share one client. `make_locked` wraps it in
      // an async `Mutex` so the periodic ping and a worker's (blocking) insert
      // never run on the same connection concurrently: the ping loop yields the
      // guard cooperatively instead of blocking a folly executor thread on the
      // client's internal `std::mutex` while an insert holds the connection.
      auto client = Option<Arc<Mutex<easy_client>>>{};
      try {
        client = easy_client::make_locked(client_args, dh);
      } catch (const panic_exception&) {
        throw;
      } catch (const ::clickhouse::OpenSSLError& e) {
        clickhouse_openssl_error_diagnostic(e.what(), args_.operator_location,
                                            tls_enabled)
          .emit(dh);
        state_->done.store(true, std::memory_order_release);
        co_return;
      } catch (const std::exception& e) {
        clickhouse_error_diagnostic(e.what(), args_.operator_location).emit(dh);
        state_->done.store(true, std::memory_order_release);
        co_return;
      }
      TENZIR_ASSERT(client);
      /// We need to wait for our workers on shutdown, so need need to keep
      /// their handles.
      state_->worker_handles.push_back(ctx.spawn_task(worker_loop(
        state_.get(), *client, tls_enabled, args_.operator_location)));
      /// We can rely on the operator's async scope cancelling our outstanding
      /// ping tasks.
      std::ignore = ctx.spawn_task(ping_loop(state_.get(), std::move(*client)));
    }
    if (state_->worker_handles.empty()) {
      state_->done.store(true, std::memory_order_release);
      co_return;
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    TENZIR_ASSERT(state_);
    if (input.rows() == 0) {
      co_return;
    }
    if (input.columns() == 0) {
      diagnostic::warning("empty event will be dropped").emit(ctx.dh());
      co_return;
    }
    input = resolve_enumerations(std::move(input));
    // Resolve the target table per row and group the slice into contiguous
    // same-table runs. Whether a column is `JSON` depends on the concrete
    // table, so serialization is deferred to the workers.
    auto& dh = ctx.dh();
    auto runs = std::vector<std::pair<std::string, table_slice>>{};
    // `split_into_table_runs` is infallible here: the callback never returns a
    // failure, so the walk always completes.
    std::ignore = split_into_table_runs(
      input, args_.table, args_.table.get_location(), dh,
      [&](std::string_view table, table_slice run) -> failure_or<void> {
        runs.emplace_back(std::string{table}, std::move(run));
        return {};
      });
    co_await state_->buffer_all(std::move(runs));
    // Backpressure: block until the workers have drained enough to bring the
    // in-memory buffer back under capacity. Suspending here stops the executor
    // from delivering the next slice, propagating pressure upstream.
    co_await state_->wait_for_capacity();
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(state_);
    // Drain all buffered rows, then signal shutdown (after the drain, so
    // workers pick up the buffered tables before the sentinels) and join.
    co_await state_->enqueue_matching([](const table_buffer&) {
      return true;
    });
    state_->done.store(true, std::memory_order_release);
    for (auto& _ : state_->worker_handles) {
      co_await state_->queue.enqueue(None{});
    }
    auto joins = std::vector<Task<void>>{};
    joins.reserve(state_->worker_handles.size());
    for (auto& handle : state_->worker_handles) {
      joins.push_back(handle.join());
    }
    co_await folly::coro::collectAllRange(std::move(joins));
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    TENZIR_ASSERT(state_);
    return state_->done.load(std::memory_order_acquire) ? OperatorState::done
                                                        : OperatorState::normal;
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(state_);
    // Drain everything before the checkpoint: enqueue all buffered tables and
    // wait until no buffer and no in-flight batch remains. After this, there is
    // nothing to persist, so `snapshot` writes nothing.
    co_await state_->enqueue_matching([](const table_buffer&) {
      return true;
    });
    co_await state_->wait_until_drained();
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // Nothing to persist: `prepare_snapshot` drains all buffered and in-flight
    // rows before the snapshot is taken.
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    // Park until some buffer has a deadline, then sleep until the earliest one
    // so `process_task` can enqueue the timed-out tables.
    auto deadline = co_await state_->earliest_deadline();
    if (not deadline) {
      co_await state_->buffer_ready.wait();
      deadline = co_await state_->earliest_deadline();
    }
    if (deadline) {
      co_await sleep_until(*deadline);
    }
    co_return {};
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(result, ctx);
    // Enqueue every table whose oldest row has timed out.
    const auto now = std::chrono::steady_clock::now();
    co_await state_->enqueue_matching([&](const table_buffer& entry) {
      return now - entry.added >= args_.batch_timeout;
    });
  }

private:
  static auto
  worker_loop(runtime_state* shared_state, Arc<Mutex<easy_client>> client,
              bool tls_enabled, location operator_location) -> Task<void> {
    TENZIR_ASSERT(shared_state);
    while (true) {
      auto next = co_await shared_state->queue.dequeue();
      // Empty Option is our shutdown signal.
      if (not next) {
        break;
      }
      const auto& table = *next;
      // Take the whole buffer for this table (marked in-flight until we finish).
      auto batch = co_await shared_state->take(table);
      if (batch.events.empty()) {
        continue;
      }
      // Do the blocking ClickHouse work off the async executor. `insert_table`
      // returns failure after emitting a diagnostic; the genuine clickhouse-cpp
      // exceptions (connection/TLS errors) still throw and are turned into a
      // failure here.
      const auto query_id = fmt::to_string(uuid::random());
      // Hold the async lock across the whole blocking insert so the ping loop
      // (which shares this client) cannot touch the connection meanwhile.
      auto guard = co_await client->lock();
      auto& c = *guard;
      auto insert = [&]() -> failure_or<void> {
        try {
          return insert_table(c, table, batch.events, query_id,
                              operator_location);
        } catch (const ::clickhouse::OpenSSLError& e) {
          clickhouse_openssl_error_diagnostic(e.what(), operator_location,
                                              tls_enabled)
            .emit(c.dh());
        } catch (const std::exception& e) {
          clickhouse_error_diagnostic(e.what(), operator_location).emit(c.dh());
        } catch (...) {
          diagnostic::error("unexpected exception").emit(c.dh());
        }
        return failure::promise();
      };
      auto result = co_await spawn_blocking(std::move(insert));
      guard.unlock();
      const auto ok = result.is_success();
      co_await shared_state->finish_insert(batch.rows, ok);
      if (ok) {
        if (batch.bytes > 0) {
          shared_state->bytes_write_counter.add(batch.bytes);
        }
        shared_state->events_write_counter.add(batch.rows);
      }
    }
  }

  /// Serializes all `JSON`-column fields, groups the events by schema, and
  /// inserts each schema group as a single block. The insert size is bounded by
  /// how much `process` accumulated per flush
  /// (`max_batch_rows`/`batch_timeout`); ClickHouse further caps the resulting
  /// part via its own `max_insert_block_size`, so we do not re-chunk here. Runs
  /// on a blocking thread (via `spawn_blocking`). Returns failure (the callee
  /// already emitted a diagnostic) so the worker can react.
  static auto insert_table(easy_client& client, std::string_view table,
                           const std::vector<table_slice>& events,
                           std::string query_id, location operator_location)
    -> failure_or<void> {
    TENZIR_UNUSED(operator_location);
    return client.insert_batch(events, table, query_id);
  }

  static auto ping_loop(runtime_state* shared_state,
                        Arc<Mutex<easy_client>> client) -> Task<void> {
    TENZIR_UNUSED(shared_state);
    while (true) {
      co_await folly::coro::sleep(schema_refresh_interval);
      // Serialize the ping against the worker's insert on the shared client.
      // The ping itself does a blocking network round-trip, so run it off the
      // async executor while holding the guard.
      auto guard = co_await client->lock();
      auto& c = *guard;
      auto ok = co_await spawn_blocking([&] {
        try {
          return c.maintain().is_success();
        } catch (const std::exception& error) {
          diagnostic::error("ClickHouse maintenance failed: {}", error.what())
            .emit(c.dh());
          return false;
        }
      });
      if (not ok) {
        co_return;
      }
      guard.unlock();
    }
  }

  ToClickhouseArgs args_;
  std::unique_ptr<runtime_state> state_;
};

class ToClickhouseEvents final : public Operator<nova::Events, void> {
public:
  explicit ToClickhouseEvents(ToClickhouseArgs args)
    : args_{std::move(args)}, window_{args_.jobs} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto options = TableOptions{};
    auto mode = from_string<enum clickhouse::mode>(args_.mode.inner);
    TENZIR_ASSERT(mode);
    options.mode = {*mode, args_.mode.source};
    if (args_.primary) {
      // The primary key was validated to be a top-level field.
      options.primary = {std::string{args_.primary->path().front().id.name},
                         args_.primary->get_location()};
    }
    // Both arguments were validated already, so parsing them cannot fail.
    if (args_.json.kind) {
      auto json = parse_json_field_argument(args_.json, dh);
      TENZIR_ASSERT(json);
      options.json = std::move(*json);
    }
    if (args_.low_cardinality.kind) {
      auto low_cardinality = parse_field_list_argument(args_.low_cardinality,
                                                       dh, "low_cardinality");
      TENZIR_ASSERT(low_cardinality);
      options.low_cardinality = std::move(*low_cardinality);
    }
    options.table = args_.table.get_location();
    auto tls = tls_options::from_optional(args_.tls).resolve(
      ctx.actor_system().config(), dh);
    if (not tls) {
      done_ = true;
      co_return;
    }
    tls_enabled_ = tls->tls.inner;
    auto connection = ConnectionArgs{};
    connection.tls = std::move(*tls);
    auto const default_port
      = tls_enabled_ ? clickhouse_tls_port : clickhouse_plaintext_port;
    connection.port = detail::narrow_cast<uint16_t>(
      args_.port ? args_.port->inner : default_port);
    auto uri = std::string{};
    auto requests = std::vector<secret_request>{};
    auto const has_uri = args_.uri.inner != secret::make_literal("");
    if (has_uri) {
      requests.push_back(make_secret_request("uri", args_.uri, uri, dh));
    } else {
      requests.push_back(
        make_secret_request("host", args_.host, connection.host, dh));
      requests.push_back(
        make_secret_request("user", args_.user, connection.user, dh));
      requests.push_back(make_secret_request("password", args_.password,
                                             connection.password, dh));
    }
    if (not co_await ctx.resolve_secrets(std::move(requests))) {
      done_ = true;
      co_return;
    }
    if (has_uri) {
      auto parsed = parse_connection_uri(uri, args_.uri.source, dh);
      if (not parsed) {
        done_ = true;
        co_return;
      }
      connection.port = detail::narrow_cast<uint16_t>(default_port);
      connection.apply_uri(*parsed);
    }
    default_database_ = connection.default_database;
    options.default_database = connection.default_database;
    auto table = co_await nova::Evaluator::make(args_.table, ctx);
    if (not table) {
      done_ = true;
      co_return;
    }
    table_.emplace(std::move(*table));
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_clickhouse"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_clickhouse"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    registry_.emplace(std::in_place, std::move(options));
    // Connecting blocks, so all connections are opened at once on the blocking
    // executor. Each attempt owns its arguments, as it may outlive `start`.
    auto attempts = std::vector<Task<Arc<Connection>>>{};
    for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
      attempts.push_back(spawn_blocking([connection] {
        return Arc<Connection>{std::in_place, connection};
      }));
    }
    for (auto i = size_t{0}; i < attempts.size(); ++i) {
      try {
        connections_.push_back(co_await std::move(attempts[i]));
      } catch (panic_exception const&) {
        throw;
      } catch (folly::OperationCancelled const&) {
        throw;
      } catch (::clickhouse::OpenSSLError const& e) {
        clickhouse_openssl_error_diagnostic(e.what(), args_.operator_location,
                                            tls_enabled_)
          .emit(dh);
        done_ = true;
        co_return;
      } catch (std::exception const& e) {
        clickhouse_error_diagnostic(e.what(), args_.operator_location).emit(dh);
        done_ = true;
        co_return;
      }
      free_.push_back(i);
      last_used_.push_back(std::chrono::steady_clock::now());
    }
    next_refresh_ = std::chrono::steady_clock::now() + schema_refresh_interval;
    arm_maintenance_timer(ctx);
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    if (done_ or not input.mask.any()) {
      co_return;
    }
    auto values = table_->eval(input, nova::EvalCtx{ctx.dh()});
    auto strings = values.get_alternative<nova::String>();
    auto valid = strings ? input.mask & strings->present
                         : nova::storage::BitMap{input.length(), false};
    if (auto invalid = input.mask.and_not(valid); invalid.any()) {
      report_invalid_tables(values, invalid, ctx);
    }
    if (not valid.any()) {
      co_return;
    }
    // Route the rows to tables by masks, so that nothing is copied.
    auto routed = std::vector<std::pair<std::string, nova::storage::BitMap>>{};
    using Constant
      = nova::storage::ConstantStorage<std::string, std::string_view>;
    if (auto const* constant = try_as<Constant>(strings->data.storage())) {
      routed.emplace_back(constant->value(), std::move(valid));
    } else {
      auto masks
        = detail::heterogeneous_string_hashmap<nova::storage::BitMap::Mutable>{};
      for (auto row : nova::storage::true_bits(valid)) {
        auto name = *strings->data.get(row);
        auto it = masks.find(name);
        if (it == masks.end()) {
          it = masks
                 .emplace(std::string{name},
                          nova::storage::BitMap::Mutable{input.length()})
                 .first;
        }
        std::ignore = it.value().set(row, true);
      }
      for (auto it = masks.begin(); it != masks.end(); ++it) {
        routed.emplace_back(it->first, std::move(it.value()).finish());
      }
    }
    for (auto& [name, mask] : routed) {
      if (not validate_table_name<false>(name, args_.table.get_location(),
                                         ctx.dh())) {
        continue;
      }
      auto table = qualified_table_name(default_database_, name);
      co_await buffer(std::move(table),
                      nova::Events{input.data, std::move(mask), input.meta},
                      ctx);
    }
    if (buffered_rows_ >= args_.max_batch_rows * (args_.jobs + 1)) {
      // Bound the memory with a full flush, which only waits for a free
      // connection.
      co_await flush_all(ctx);
    }
    arm_flush_timer(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await wakeup_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    if (result.try_as<RequestDone>()) {
      poll(ctx);
      co_return;
    }
    if (auto* due = result.try_as<MaintenanceDue>()) {
      if (due->generation == maintenance_generation_) {
        maintenance_deadline_ = None{};
        maintain(ctx);
      }
      co_return;
    }
    TENZIR_ASSERT(result.try_as<FlushTimeout>());
    timer_armed_ = false;
    auto const now = std::chrono::steady_clock::now();
    auto expired = std::vector<std::string>{};
    for (auto const& [table, buffer] : buffers_) {
      if (buffer.deadline <= now) {
        expired.push_back(table);
      }
    }
    for (auto const& table : expired) {
      co_await flush(table, ctx);
    }
    arm_flush_timer(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await flush_all(ctx);
    co_await drain(ctx);
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    // Nothing remains to be persisted once everything is inserted.
    co_await flush_all(ctx);
    co_await drain(ctx);
  }

private:
  /// The events buffered for one table.
  struct TableBuffer {
    std::vector<nova::Events> events;
    uint64_t rows = 0;
    uint64_t bytes = 0;
    std::chrono::steady_clock::time_point deadline;
  };

  /// The outcome of one request on a connection.
  struct Completion {
    size_t connection = 0;
    uint64_t rows = 0;
    uint64_t bytes = 0;
    std::vector<diagnostic> diagnostics;
    bool ok = false;
    /// When the request last talked to the server, if it did.
    Option<std::chrono::steady_clock::time_point> contacted = None{};
    /// When the schemas are due for a refresh again, after a refresh.
    Option<std::chrono::steady_clock::time_point> next_refresh = None{};
  };

  /// Wakeup marker for the flush timer.
  struct FlushTimeout {};

  /// Wakeup marker for a finished request.
  struct RequestDone {};

  /// Wakeup marker for the maintenance timer of `generation`.
  struct MaintenanceDue {
    uint64_t generation = 0;
  };

  auto report_invalid_tables(nova::Array<nova::Data> const& values,
                             nova::storage::BitMap const& invalid,
                             OpCtx& ctx) const -> void {
    auto report = [&]<class Tag>(nova::Array<Tag> const&) {
      if constexpr (not std::same_as<Tag, nova::String>) {
        diagnostic::warning("expected `string`, got `{}`",
                            nova::Type<Tag>::static_name)
          .primary(args_.table)
          .note("event is skipped")
          .emit(ctx);
      }
    };
    match(values, report, [&](nova::UnionArray const& alternatives) {
      for (auto const& alternative : alternatives.fields()) {
        if ((invalid & alternative.present).any()) {
          match(alternative.data, report);
        }
      }
    });
  }

  auto buffer(std::string table, nova::Events events, OpCtx& ctx)
    -> Task<void> {
    auto const rows = static_cast<uint64_t>(events.active_count());
    auto const bytes
      = events.approx_bytes() * rows
        / std::max(uint64_t{1}, static_cast<uint64_t>(events.length()));
    auto it = buffers_.find(table);
    if (it == buffers_.end()) {
      it = buffers_
             .emplace(table,
                      TableBuffer{
                        .events = {},
                        .rows = 0,
                        .bytes = 0,
                        .deadline = std::chrono::steady_clock::now()
                                    + std::chrono::duration_cast<
                                      std::chrono::steady_clock::duration>(
                                      args_.batch_timeout),
                      })
             .first;
    }
    auto& entry = it.value();
    entry.events.push_back(std::move(events));
    entry.rows += rows;
    entry.bytes += bytes;
    buffered_rows_ += rows;
    if (entry.rows >= args_.max_batch_rows) {
      co_await flush(table, ctx);
    }
  }

  auto flush_all(OpCtx& ctx) -> Task<void> {
    auto tables = std::vector<std::string>{};
    for (auto const& [table, buffer] : buffers_) {
      tables.push_back(table);
    }
    for (auto const& table : tables) {
      co_await flush(table, ctx);
    }
  }

  /// The wakeup for a finished request.
  auto request_done() const {
    return [queue = wakeup_queue_]() mutable {
      // A full queue already holds wakeups, so the completion is polled soon
      // anyway.
      std::ignore = queue->try_enqueue(Any{RequestDone{}});
    };
  }

  /// Hands the buffer of `table` to an insert job.
  auto flush(std::string const& table, OpCtx& ctx) -> Task<void> {
    auto it = buffers_.find(table);
    if (it == buffers_.end()) {
      co_return;
    }
    auto buffer = std::move(it.value());
    buffers_.erase(it);
    buffered_rows_ -= buffer.rows;
    co_await window_.acquire([&](Completion completion) {
      handle_completion(std::move(completion), ctx);
    });
    if (done_) {
      co_return;
    }
    TENZIR_ASSERT(not free_.empty());
    auto const index = free_.back();
    free_.pop_back();
    window_.spawn(
      ctx,
      [connection = connections_[index], index, registry = *registry_, table,
       buffer = std::move(buffer), tls_enabled = tls_enabled_,
       location = args_.operator_location]() mutable -> Task<Completion> {
        return insert_job(std::move(connection), index, std::move(registry),
                          std::move(table), std::move(buffer.events),
                          buffer.bytes, tls_enabled, location);
      },
      request_done());
  }

  /// Starts the maintenance that is due on idle connections. Refreshing the
  /// schemas also keeps its connection alive, so it takes the connection that
  /// was idle longest.
  auto maintain(OpCtx& ctx) -> void {
    if (done_) {
      return;
    }
    auto const now = std::chrono::steady_clock::now();
    auto const ping_due = [&](size_t index) {
      return now >= last_used_[index] + connection_ping_interval;
    };
    if ((*registry_)->has_catch_all() and now >= next_refresh_
        and not free_.empty()) {
      auto const oldest = std::ranges::min_element(free_, {}, [&](size_t i) {
        return last_used_[i];
      });
      auto const index = *oldest;
      free_.erase(oldest);
      next_refresh_ = std::chrono::steady_clock::time_point::max();
      spawn_maintenance(index, *registry_, ping_due(index), ctx);
    }
    auto due = std::vector<size_t>{};
    std::erase_if(free_, [&](size_t index) {
      if (not ping_due(index)) {
        return false;
      }
      due.push_back(index);
      return true;
    });
    for (auto index : due) {
      spawn_maintenance(index, None{}, true, ctx);
    }
    arm_maintenance_timer(ctx);
  }

  /// Runs a maintenance job on the connection `index`, which must be free.
  auto spawn_maintenance(size_t index, Option<Arc<TableRegistry>> registry,
                         bool ping, OpCtx& ctx) -> void {
    window_.spawn(
      ctx,
      [connection = connections_[index], index, registry = std::move(registry),
       ping]() mutable -> Task<Completion> {
        return maintenance_job(std::move(connection), index,
                               std::move(registry), ping);
      },
      request_done());
  }

  /// Arms a timer for the next maintenance of an idle connection, unless one
  /// fires earlier already. Busy connections are skipped, because their
  /// completion arms the timer again.
  auto arm_maintenance_timer(OpCtx& ctx) -> void {
    if (done_ or free_.empty()) {
      return;
    }
    auto deadline = (*registry_)->has_catch_all()
                      ? next_refresh_
                      : std::chrono::steady_clock::time_point::max();
    for (auto index : free_) {
      deadline
        = std::min(deadline, last_used_[index] + connection_ping_interval);
    }
    if (maintenance_deadline_ and *maintenance_deadline_ <= deadline) {
      return;
    }
    maintenance_deadline_ = deadline;
    ++maintenance_generation_;
    std::ignore = ctx.spawn_task(
      [queue = wakeup_queue_, deadline,
       generation = maintenance_generation_]() mutable -> Task<void> {
        co_await sleep_until(deadline);
        co_await queue->enqueue(Any{MaintenanceDue{generation}});
      });
  }

  auto arm_flush_timer(OpCtx& ctx) -> void {
    if (timer_armed_ or buffers_.empty()) {
      return;
    }
    auto deadline = std::chrono::steady_clock::time_point::max();
    for (auto const& [table, buffer] : buffers_) {
      deadline = std::min(deadline, buffer.deadline);
    }
    timer_armed_ = true;
    std::ignore = ctx.spawn_task(
      [queue = wakeup_queue_, deadline]() mutable -> Task<void> {
        co_await sleep_until(deadline);
        co_await queue->enqueue(Any{FlushTimeout{}});
      });
  }

  auto drain(OpCtx& ctx) -> Task<void> {
    co_await window_.drain([&](Completion completion) {
      handle_completion(std::move(completion), ctx);
    });
  }

  auto poll(OpCtx& ctx) -> void {
    window_.poll([&](Completion completion) {
      handle_completion(std::move(completion), ctx);
    });
  }

  /// Emits diagnostics from a task, pointing them at the operator.
  auto emit(std::vector<diagnostic> diagnostics, OpCtx& ctx) const -> void {
    for (auto& diag : diagnostics) {
      if (not has_location(diag)) {
        diag.annotations.emplace_back(true, std::string{},
                                      args_.operator_location);
      }
      ctx.dh().emit(std::move(diag));
    }
  }

  auto handle_completion(Completion completion, OpCtx& ctx) -> void {
    free_.push_back(completion.connection);
    if (completion.contacted) {
      last_used_[completion.connection] = *completion.contacted;
    }
    if (completion.next_refresh) {
      next_refresh_ = *completion.next_refresh;
    }
    emit(std::move(completion.diagnostics), ctx);
    if (not completion.ok) {
      done_ = true;
      return;
    }
    if (completion.bytes > 0) {
      bytes_write_counter_.add(completion.bytes);
    }
    events_write_counter_.add(completion.rows);
    arm_maintenance_timer(ctx);
  }

  /// Inserts `events` into `table`. Runs concurrently with the operator.
  static auto insert_job(Arc<Connection> connection, size_t index,
                         Arc<TableRegistry> registry, std::string table,
                         std::vector<nova::Events> events, uint64_t bytes,
                         bool tls_enabled, location operator_location)
    -> Task<Completion> {
    auto dh = collecting_diagnostic_handler{};
    auto rows = failure_or<uint64_t>{failure::promise()};
    try {
      rows = co_await send_inserts(*connection, *registry, table, events, dh);
    } catch (panic_exception const&) {
      throw;
    } catch (folly::OperationCancelled const&) {
      throw;
    } catch (::clickhouse::OpenSSLError const& e) {
      clickhouse_openssl_error_diagnostic(e.what(), operator_location,
                                          tls_enabled)
        .emit(dh);
    } catch (std::exception const& e) {
      clickhouse_error_diagnostic(e.what(), operator_location).emit(dh);
    }
    co_return Completion{
      .connection = index,
      .rows = rows ? *rows : 0,
      .bytes = bytes,
      .diagnostics = std::move(dh).collect(),
      .ok = rows.is_success(),
      // A failed insert resets the connection, which also talks to the server.
      .contacted = std::chrono::steady_clock::now(),
    };
  }

  /// Sends one insert per group of rows and returns the number of inserted
  /// rows. Catch-all tables retry the remaining groups once with a fresh
  /// schema if the server rejects the current one.
  static auto
  send_inserts(Connection& connection, TableRegistry& registry,
               std::string const& table,
               std::vector<nova::Events> const& events, diagnostic_handler& dh)
    -> Task<failure_or<uint64_t>> {
    CO_TRY(auto schema, co_await registry.get(table, connection, events, dh));
    auto inserts = build_inserts(schema->root, events, table, dh);
    auto retry_events = std::vector<nova::Events>{};
    auto retried = false;
    auto next = size_t{0};
    auto inserted = uint64_t{0};
    while (next < inserts.size()) {
      auto rejection = std::string{};
      try {
        auto const& block = inserts[next].block;
        auto const catch_all = schema->root.catch_all.has_value();
        co_await spawn_blocking([&] {
          plugins::clickhouse::insert(connection, block, table,
                                      fmt::to_string(uuid::random()), catch_all,
                                      dh);
        });
        // Acknowledged rows are never part of a retry.
        inserted += inserts[next].rows;
        ++next;
        continue;
      } catch (::clickhouse::ServerException const& error) {
        using enum ::clickhouse::ErrorCodes;
        auto const code = error.GetCode();
        // Only these rejections are known to happen before the server accepts
        // a block, so only they can be replayed.
        auto const replayable = code == NO_SUCH_COLUMN_IN_TABLE
                                or code == NOT_FOUND_COLUMN_IN_BLOCK
                                or code == TYPE_MISMATCH;
        if (retried or not schema->root.catch_all or not replayable) {
          throw;
        }
        rejection = error.what();
      }
      auto updated = failure_or<Option<Arc<TableSchema const>>>{};
      try {
        updated = co_await registry.invalidate(table, schema, connection, dh);
      } catch (std::exception const& error) {
        diagnostic::error("metadata refresh failed: {}", error.what()).emit(dh);
        updated = failure::promise();
      }
      if (not updated) {
        diagnostic::error("insertion rejected: {}", rejection).emit(dh);
        co_return failure::promise();
      }
      if (not *updated) {
        diagnostic::error("ClickHouse error: {}", rejection).emit(dh);
        co_return failure::promise();
      }
      schema = std::move(**updated);
      retried = true;
      auto remaining = std::vector<nova::Events>{};
      remaining.reserve(events.size());
      for (auto p = size_t{0}; p < events.size(); ++p) {
        auto mask = nova::storage::BitMap{events[p].length(), false};
        for (auto i = next; i < inserts.size(); ++i) {
          mask = std::move(mask) | inserts[i].input_rows[p];
        }
        remaining.emplace_back(events[p].data, std::move(mask), events[p].meta);
      }
      retry_events = std::move(remaining);
      inserts = build_inserts(schema->root, retry_events, table, dh);
      next = 0;
    }
    co_return inserted;
  }

  /// Refreshes the expired schemas of `registry`, if given. Pings the server if
  /// `ping` is set and the refresh did not talk to it.
  static auto maintenance_job(Arc<Connection> connection, size_t index,
                              Option<Arc<TableRegistry>> registry, bool ping)
    -> Task<Completion> {
    auto dh = collecting_diagnostic_handler{};
    auto ok = true;
    auto contacted = Option<std::chrono::steady_clock::time_point>{};
    auto next_refresh = Option<std::chrono::steady_clock::time_point>{};
    try {
      if (registry) {
        auto refresh = co_await (*registry)->refresh_expired(*connection, dh);
        ok = refresh.is_success();
        if (ok) {
          auto const now = std::chrono::steady_clock::now();
          if (refresh->contacted) {
            contacted = now;
          }
          next_refresh = refresh->next.value_or(now + schema_refresh_interval);
        }
      }
      if (ok and ping and not contacted) {
        co_await spawn_blocking([&] {
          connection->client().Ping();
        });
        contacted = std::chrono::steady_clock::now();
      }
    } catch (panic_exception const&) {
      throw;
    } catch (folly::OperationCancelled const&) {
      throw;
    } catch (std::exception const& e) {
      diagnostic::error("ClickHouse maintenance failed: {}", e.what()).emit(dh);
      ok = false;
    }
    co_return Completion{
      .connection = index,
      .diagnostics = std::move(dh).collect(),
      .ok = ok,
      .contacted = contacted,
      .next_refresh = next_refresh,
    };
  }

  ToClickhouseArgs args_;
  Option<nova::Evaluator> table_;
  Option<std::string> default_database_;
  bool tls_enabled_ = false;
  Option<Arc<TableRegistry>> registry_;
  std::vector<Arc<Connection>> connections_;
  /// Indices of connections without a request.
  std::vector<size_t> free_;
  /// When each connection last talked to the server.
  std::vector<std::chrono::steady_clock::time_point> last_used_;
  /// When the schemas of catch-all tables are due for a refresh. The maximum
  /// while a refresh runs.
  std::chrono::steady_clock::time_point next_refresh_;
  /// Bounds the requests in flight, one per connection.
  RequestWindow<Completion> window_;
  detail::heterogeneous_string_hashmap<TableBuffer> buffers_;
  uint64_t buffered_rows_ = 0;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
  /// Whether a flush-timer task is outstanding.
  bool timer_armed_ = false;
  /// When the current maintenance timer fires, if one is armed.
  Option<std::chrono::steady_clock::time_point> maintenance_deadline_;
  /// Identifies the current maintenance timer, so that replaced ones are
  /// ignored.
  uint64_t maintenance_generation_ = 0;
  bool done_ = false;
  /// Wakeups for `await_task`. Helper tasks enqueue, and only the operator
  /// dequeues.
  mutable Arc<folly::coro::BoundedQueue<Any>> wakeup_queue_{std::in_place, 16};
};

class to_clickhouse final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_clickhouse";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToClickhouseArgs, ToClickhouse, ToClickhouseEvents>{};
    auto uri_arg = d.named_optional("uri", &ToClickhouseArgs::uri);
    auto host_arg = d.named_optional("host", &ToClickhouseArgs::host);
    auto port_arg = d.named("port", &ToClickhouseArgs::port);
    auto user_arg = d.named_optional("user", &ToClickhouseArgs::user);
    auto password_arg
      = d.named_optional("password", &ToClickhouseArgs::password);
    auto table_arg = d.named("table", &ToClickhouseArgs::table, "string");
    auto mode_arg = d.named_optional("mode", &ToClickhouseArgs::mode);

    auto primary_arg = d.named("primary", &ToClickhouseArgs::primary, "field");
    auto json_arg
      = d.named_optional("json", &ToClickhouseArgs::json, "field|list<field>");
    auto low_cardinality_arg
      = d.named_optional("low_cardinality", &ToClickhouseArgs::low_cardinality,
                         "field|list<field>");
    auto jobs_arg = d.named_optional("_jobs", &ToClickhouseArgs::jobs);
    auto max_batch_rows_arg
      = d.named_optional("max_batch_rows", &ToClickhouseArgs::max_batch_rows);
    auto batch_timeout_arg = d.named_optional(
      "batch_timeout", &ToClickhouseArgs::batch_timeout, "duration");
    auto tls_validate
      = tls_options{}.add_to_describer(d, &ToClickhouseArgs::tls);
    d.operator_location(&ToClickhouseArgs::operator_location);
    d.validate(
      [uri_arg, host_arg, table_arg, mode_arg, port_arg, user_arg, password_arg,
       primary_arg, json_arg, low_cardinality_arg, jobs_arg, max_batch_rows_arg,
       batch_timeout_arg,
       tls_validate = std::move(tls_validate)](DescribeCtx& ctx) -> Empty {
        tls_validate(ctx);
        auto has_uri = ctx.get(uri_arg).has_value();
        auto has_host = ctx.get(host_arg).has_value();
        auto has_port = ctx.get(port_arg).has_value();
        auto has_user = ctx.get(user_arg).has_value();
        auto has_password = ctx.get(password_arg).has_value();
        if (has_uri and (has_host or has_port or has_user or has_password)) {
          diagnostic::error(
            "`uri` and explicit connection arguments are mutually exclusive")
            .primary(ctx.get_location(uri_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
        if (auto port = ctx.get(port_arg)) {
          if (port->inner == 0 or port->inner > 65535) {
            diagnostic::error("`port` must be between 1 and 65535")
              .primary(port->source, "got `{}`", port->inner)
              .emit(ctx);
          }
        }
        auto mode_enum = Option<enum mode>{};
        if (auto mode_opt = ctx.get(mode_arg)) {
          if (auto x = from_string<enum mode>(mode_opt->inner)) {
            mode_enum = x;
          } else {
            diagnostic::error(
              "`mode` must be one of `create`, `append` or `create_append`")
              .primary(mode_opt->source, "got `{}`", mode_opt->inner)
              .emit(ctx);
          }
        }
        if (auto jobs = ctx.get(jobs_arg)) {
          if (*jobs == 0) {
            diagnostic::error("`_jobs` must be larger than 0")
              .primary(*ctx.get_location(jobs_arg))
              .emit(ctx);
          }
          if (*jobs > 1 and mode_enum != mode::append) {
            diagnostic::error("can only specify jobs > 1 with `mode` `append`")
              .primary(*ctx.get_location(jobs_arg))
              .primary(ctx.get_location(mode_arg).value_or(location::unknown))
              .emit(ctx);
          }
        }
        if (auto max_batch_rows = ctx.get(max_batch_rows_arg)) {
          if (*max_batch_rows < 1 or *max_batch_rows > 100'000) {
            diagnostic::error("`max_batch_rows` must be in [1, 100'000]")
              .primary(*ctx.get_location(max_batch_rows_arg))
              .emit(ctx);
          }
        }
        if (auto batch_timeout = ctx.get(batch_timeout_arg)) {
          if (*batch_timeout <= duration::zero()) {
            diagnostic::error("`batch_timeout` must be a positive duration")
              .primary(*ctx.get_location(batch_timeout_arg))
              .emit(ctx);
          }
        }
        if (auto table = ctx.get(table_arg)) {
          auto sp = session_provider::make(ctx);
          if (auto table_name = try_const_eval(*table, sp.as_session())) {
            if (const auto* s = try_as<std::string>(table_name->inner)) {
              (void)validate_table_name<true>(*s, table->get_location(), ctx);
            } else {
              diagnostic::error("`table` must be a `string`")
                .primary(table->get_location())
                .emit(ctx);
            }
          }
        }
        if (auto primary = ctx.get(primary_arg)) {
          auto p = primary->path();
          if (p.size() > 1) {
            diagnostic::error("`primary`, must be a top level field")
              .primary(primary->get_location())
              .emit(ctx);
          }
          if (not validate_identifier(p.front().id.name)) {
            emit_invalid_identifier<true>("primary", p.front().id.name,
                                          primary->get_location(), ctx);
          }
        }
        if (mode_enum == mode::create and not ctx.get(primary_arg)) {
          diagnostic::error("mode `create` requires `primary` to be set")
            .primary(ctx.get_location(mode_arg).value_or(location::unknown))
            .emit(ctx);
        }
        if (auto json = ctx.get(json_arg)) {
          // `json` serves two purposes: when creating a table it forces the
          // listed fields (at any nesting depth) to the ClickHouse `JSON`
          // type, and in all modes it makes the operator serialize the
          // listed top-level fields to opaque JSON strings before insertion.
          // The latter collapses heterogeneous input into a single Tenzir
          // schema so batching can coalesce it, and is essential on the
          // high-volume `mode = "append"` path.
          if (auto columns = parse_json_field_argument(*json, ctx)) {
            if (auto primary = ctx.get(primary_arg)) {
              // `primary` is always a top-level field (validated above). Any
              // `json` entry rooted at `primary`, whether an exact match or
              // nested beneath it, would place a `JSON`-typed value inside
              // the primary key's `Tuple`, which ClickHouse cannot use as a
              // sort key.
              const auto primary_name
                = std::string{primary->path().front().id.name};
              for (const auto& column : *columns) {
                if (column.inner.front() == primary_name) {
                  diagnostic::error("a `JSON` column cannot be the primary "
                                    "key or nested beneath it")
                    .primary(column.source)
                    .primary(primary->get_location())
                    .emit(ctx);
                }
              }
            }
          }
        }
        if (auto low_cardinality = ctx.get(low_cardinality_arg)) {
          // `low_cardinality` only takes effect when creating a table.
          if (mode_enum == mode::append) {
            diagnostic::error(
              "`low_cardinality` cannot be used with `mode = \"append\"`")
              .primary(low_cardinality->get_location())
              .primary(ctx.get_location(mode_arg).value_or(location::unknown))
              .note("`low_cardinality` only applies when creating a table")
              .emit(ctx);
          }
          if (auto columns = parse_field_list_argument(*low_cardinality, ctx,
                                                       "low_cardinality")) {
            // A field cannot be both a `JSON` and a `LowCardinality` column.
            if (auto json = ctx.get(json_arg)) {
              if (auto json_columns = parse_json_field_argument(*json, ctx)) {
                for (const auto& lc : *columns) {
                  for (const auto& jc : *json_columns) {
                    if (lc.inner == jc.inner) {
                      diagnostic::error("column `{}` cannot be both `json` and "
                                        "`low_cardinality`",
                                        to_dotted_string(lc.inner))
                        .primary(lc.source)
                        .primary(jc.source)
                        .emit(ctx);
                    }
                  }
                }
              }
            }
          }
        }
        return {};
      });
    return d.unordered();
  }
};

} // namespace
} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::to_clickhouse)
