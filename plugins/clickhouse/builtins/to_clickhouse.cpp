//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/easy_client.hpp"
#include "clickhouse/exceptions.h"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/concept/printable/tenzir/json2.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/view3.hpp"

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <fmt/format.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>

#include <memory>

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

namespace {

constexpr auto clickhouse_plaintext_port = uint64_t{9000};
constexpr auto clickhouse_tls_port = uint64_t{9440};
/// Must stay below ClickHouse's server-side `receive_timeout`, which defaults
/// to 300 seconds in `src/Core/Defines.h` as
/// `DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC` and is applied to native TCP connections
/// in `src/Server/TCPHandler.cpp` via `socket().setReceiveTimeout(...)`.
constexpr auto clickhouse_ping_interval = std::chrono::minutes{3};
static_assert(clickhouse_ping_interval < std::chrono::minutes{5},
              "clickhouse_ping_interval must stay below 5 minutes");

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

class clickhouse_sink_operator final
  : public crtp_operator<clickhouse_sink_operator> {
public:
  clickhouse_sink_operator() = default;

  clickhouse_sink_operator(operator_arguments args) : args_{std::move(args)} {
  }

  friend auto inspect(auto& f, clickhouse_sink_operator& x) -> bool {
    return f.apply(x.args_);
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return {{}, event_order::unordered, copy()};
  }

  auto name() const -> std::string override {
    return "to_clickhouse";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto detached() const -> bool override {
    return true;
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> try {
    auto& dh = ctrl.diagnostics();
    auto ssl_opts = args_.ssl;
    auto ssl_result = ssl_opts.resolve(ctrl);
    if (not ssl_result) {
      co_return;
    }
    auto const tls_enabled = ssl_result->tls.inner;
    auto const default_port
      = tls_enabled ? clickhouse_tls_port : clickhouse_plaintext_port;
    auto args = easy_client::arguments{
      .host = "",
      .port = args_.port ? *args_.port
                         : located<uint64_t>{default_port, location::unknown},
      .user = "default",
      .password = "",
      .default_database = None{},
      .set_client_default_database = false,
      .ssl = std::move(*ssl_result),
      .table = args_.table,
      .mode = args_.mode,
      .primary = args_.primary,
      .operator_location = args_.operator_location,
    };
    auto uri = std::string{};
    auto requests = std::vector<secret_request>{};
    auto has_uri = args_.uri && args_.uri->inner != secret::make_literal("");
    if (has_uri) {
      requests.push_back(make_secret_request("uri", *args_.uri, uri, dh));
    } else {
      requests.push_back(
        make_secret_request("host", args_.host, args.host, dh));
      requests.push_back(
        make_secret_request("user", args_.user, args.user, dh));
      requests.push_back(
        make_secret_request("password", args_.password, args.password, dh));
    }
    /// GCC 14.2 erroneously warns that the first temporary here may used as a
    /// dangling pointer at the end/suspension of the coroutine. Giving `x` a
    /// name somehow circumvents this warning.
    auto x = ctrl.resolve_secrets_must_yield(std::move(requests));
    co_yield std::move(x);
    if (has_uri) {
      auto parsed = parse_connection_uri(uri, args_.uri->source, dh);
      if (not parsed) {
        co_return;
      }
      apply_connection_uri(args, *parsed);
      if (not parsed->has_port()) {
        args.port = located<uint64_t>{default_port, location::unknown};
      }
    }
    auto client = easy_client::make(args, ctrl.diagnostics());
    if (not client) {
      co_return;
    }
    auto disp = detail::weak_run_delayed_loop(
      &ctrl.self(), clickhouse_ping_interval,
      [&client]() {
        client->ping();
      },
      false);
    const auto guard
      = detail::scope_guard([disp = std::move(disp)]() mutable noexcept {
          disp.dispose();
        });
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (slice.columns() == 0) {
        diagnostic::warning("empty event will be dropped")
          .primary(args.operator_location)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      slice = resolve_enumerations(slice);
      if (not client->insert_dynamic(slice)) {
        co_return;
      }
      co_yield {};
    }
  } catch (const panic_exception& e) {
    throw;
  } catch (const ::clickhouse::OpenSSLError& e) {
    // The original `ssl` from `operator()` is out of scope by the time the
    // catch handler runs; resolve a fresh `TlsConfig` here just for the
    // diagnostic-hint TLS-enabled flag.
    auto ssl_opts = args_.ssl;
    auto resolved = ssl_opts.resolve(ctrl);
    auto tls_enabled = resolved and resolved->tls.inner;
    clickhouse_openssl_error_diagnostic(e.what(), args_.operator_location,
                                        tls_enabled)
      .emit(ctrl.diagnostics());
    co_return;
  } catch (const std::exception& e) {
    auto diag = diagnostic::error("ClickHouse error: {}", e.what())
                  .primary(args_.operator_location);
    auto ssl_opts = args_.ssl;
    auto resolved = ssl_opts.resolve(ctrl);
    auto tls_enabled = resolved and resolved->tls.inner;
    add_tls_client_diagnostic_hints(std::move(diag), tls_enabled, "ClickHouse",
                                    clickhouse_plaintext_port,
                                    clickhouse_tls_port)
      .emit(ctrl.diagnostics());
    co_return;
  }

private:
  operator_arguments args_;
};

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
  uint64_t max_batch_rows = 65536;
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
    auto json_columns = std::vector<located<std::string>>{};
    if (args_.json.kind) {
      // The `json` argument was already validated during parsing, so
      // re-extracting the column names here cannot fail. These only affect
      // table *creation* (the columns are created as `JSON`); the worker
      // discovers which columns are JSON from the actual table and serializes
      // accordingly.
      auto parsed = parse_json_field_argument(args_.json, dh);
      TENZIR_ASSERT(parsed);
      json_columns = std::move(*parsed);
    }
    auto low_cardinality_columns = std::vector<located<std::string>>{};
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
    // Group by schema so `insert` gets single-schema slices. Serializing JSON
    // columns and dropping table-absent columns (below) may collapse several
    // input schemas into one.
    auto by_schema = std::unordered_map<type, std::vector<table_slice>>{};
    for (const auto& original : events) {
      // Discover the target table's columns (blocking DESCRIBE/CREATE), then
      // serialize JSON fields and drop columns the table does not accept.
      TRY(auto tr, client.ensure_transformations(
                     as<record_type>(original.schema()), table));
      auto slice = prepare_slice(original, *tr, client.dh(), operator_location);
      auto schema = slice.schema();
      by_schema[std::move(schema)].push_back(std::move(slice));
    }
    for (auto& [schema, slices] : by_schema) {
      TRY(client.insert(concatenate(std::move(slices)), table, query_id));
    }
    return {};
  }

  /// Rewrites `slice` for insertion into the target table described by `tr`:
  /// - top-level fields that target a ClickHouse `JSON` column are replaced
  /// with
  ///   their opaque JSON-string rendering (a field already of type `string` is
  ///   left unchanged, assumed to be JSON);
  /// - top-level columns the table does not accept (unknown, or generated
  ///   `MATERIALIZED`/`ALIAS` columns) are dropped with a warning.
  ///
  /// Dropping table-absent columns here — before the by-schema grouping in
  /// `insert_table` — lets events that differ only in such columns collapse
  /// into one schema and batch together.
  static auto
  prepare_slice(const table_slice& slice, const transformer_record& tr,
                diagnostic_handler& dh, location operator_location)
    -> table_slice {
    auto fields = std::vector<record_type::field_view>{};
    auto arrays = arrow::ArrayVector{};
    auto changed = false;
    for (const auto& column : columns_of(slice)) {
      const auto trafo = tr.transfrom_and_index_for(column.name).trafo;
      if (not trafo) {
        // The column is not a writable target column: drop it (and warn),
        // shrinking the schema so more slices coalesce.
        if (tr.generated_columns.contains(column.name)) {
          diagnostic::warning("column `{}` is a generated ClickHouse column "
                              "and "
                              "cannot be written",
                              column.name)
            .note("the provided value is ignored; ClickHouse computes the "
                  "column")
            .primary(operator_location)
            .emit(dh);
        } else {
          diagnostic::warning("column `{}` does not exist in the ClickHouse "
                              "table",
                              column.name)
            .note("column will be dropped")
            .primary(operator_location)
            .emit(dh);
        }
        changed = true;
        continue;
      }
      if (is_json_transformer(*trafo)
          and not column.type.kind().is<string_type>()) {
        fields.emplace_back(column.name, type{string_type{}});
        arrays.push_back(to_json_string_array(column.array));
        changed = true;
      } else {
        fields.emplace_back(column.name, column.type);
        arrays.push_back(column.array.Slice(0));
      }
    }
    if (not changed) {
      return slice;
    }
    auto new_schema = type{"tenzir.clickhouse-prepared", record_type{fields}};
    auto batch
      = arrow::RecordBatch::Make(new_schema.to_arrow_schema(),
                                 detail::narrow_cast<int64_t>(slice.rows()),
                                 std::move(arrays));
    auto result = table_slice{batch, std::move(new_schema)};
    result.offset(slice.offset());
    result.import_time(slice.import_time());
    return result;
  }

  static auto ping_loop(runtime_state* shared_state,
                        Arc<Mutex<easy_client>> client) -> Task<void> {
    TENZIR_UNUSED(shared_state);
    while (true) {
      co_await folly::coro::sleep(clickhouse_ping_interval);
      // Serialize the ping against the worker's insert on the shared client.
      // The ping itself does a blocking network round-trip, so run it off the
      // async executor while holding the guard.
      auto guard = co_await client->lock();
      auto& c = *guard;
      co_await spawn_blocking([&] {
        c.ping();
      });
      guard.unlock();
    }
  }

  ToClickhouseArgs args_;
  std::unique_ptr<runtime_state> state_;
};

class to_clickhouse final
  : public virtual operator_plugin2<clickhouse_sink_operator>,
    public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, operator_arguments::try_parse(name(), inv, ctx));
    return std::make_unique<clickhouse_sink_operator>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<ToClickhouseArgs, ToClickhouse>{};
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
            if (const auto* s = try_as<std::string>(*table_name)) {
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
          // listed columns to the ClickHouse `JSON` type, and in all modes it
          // makes the operator serialize the listed top-level fields to opaque
          // JSON strings before insertion. The latter collapses heterogeneous
          // input into a single Tenzir schema so batching can coalesce it, and
          // is essential on the high-volume `mode = "append"` path.
          if (auto columns = parse_json_field_argument(*json, ctx)) {
            if (auto primary = ctx.get(primary_arg)) {
              const auto primary_name
                = std::string{primary->path().front().id.name};
              for (const auto& column : *columns) {
                if (column.inner == primary_name) {
                  diagnostic::error("a `JSON` column cannot be the primary key")
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
            // A column cannot be both a `JSON` and a `LowCardinality` column.
            if (auto json = ctx.get(json_arg)) {
              if (auto json_columns = parse_json_field_argument(*json, ctx)) {
                for (const auto& lc : *columns) {
                  for (const auto& jc : *json_columns) {
                    if (lc.inner == jc.inner) {
                      diagnostic::error("column `{}` cannot be both `json` and "
                                        "`low_cardinality`",
                                        lc.inner)
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
