//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/catalog.hpp"
#include "tenzir/plugins/iceberg/catalog_args.hpp"
#include "tenzir/plugins/iceberg/detail/arrow_read.hpp"
#include "tenzir/plugins/iceberg/scan.hpp"

#include <tenzir/any.hpp>
#include <tenzir/arc.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/async/result.hpp>
#include <tenzir/async/scope.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/hash/sha.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator/optimization.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/pushdown/read.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/variant.hpp>

#include <fmt/format.h>
#include <folly/CancellationToken.h>
#include <folly/coro/Sleep.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/table.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

constexpr auto default_poll_interval = std::chrono::seconds{10};
constexpr auto max_transient_attempts = 5;
constexpr auto initial_retry_backoff = std::chrono::milliseconds{250};
constexpr auto message_queue_capacity = uint32_t{8};
constexpr auto max_concurrent_files = size_t{8};
constexpr auto per_file_queue_capacity = uint32_t{4};

struct FromIcebergArgs : CatalogArgs {
  Option<located<std::string>> decimal_format;
  Option<located<int64_t>> snapshot_id;
  Option<located<time>> as_of;
  Option<located<std::string>> ref;
  Option<located<bool>> live;
  Option<located<duration>> poll_interval;
  Option<located<int64_t>> from_snapshot_id;
  Option<located<int64_t>> to_snapshot_id;
  location operator_location;
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection> optimization;
};

auto parse_decimal_format(std::string_view value) -> Option<DecimalFormat> {
  if (value == "string") {
    return DecimalFormat::string;
  }
  if (value == "float") {
    return DecimalFormat::float_;
  }
  return None{};
}

/// One converted batch of events from a scan task.
struct BatchMessage {
  nova::Events events;
  /// Number of unskipped raw rows in `events` (before local filter/limit).
  uint64_t raw_rows = 0;
  /// Bytes read for this file (credited on the first batch of the file).
  uint64_t bytes = 0;
  /// Data file path and scan range (populated on the first batch of a task for
  /// mid-file checkpoint resume verification).
  std::string file_path;
  int64_t start = 0;
  int64_t length = 0;
};

/// Chains a completed scan task's `(file_path, start, length)` into a
/// fixed-size SHA-256 digest so checkpoint resume verifies the exact prefix of
/// skipped scan ranges in O(1) checkpoint state, including at file boundaries.
auto chain_file_digest(std::string_view prev_digest, std::string_view file_path,
                       int64_t start, int64_t length) -> std::string {
  auto const digest
    = tenzir::hash<tenzir::sha256>(prev_digest.size(), prev_digest,
                                   file_path.size(), file_path, start, length);
  return std::string{reinterpret_cast<char const*>(digest.data()),
                     digest.size()};
}

/// Signals completion of the current scan task so checkpoint state advances.
struct FileDoneMessage {
  /// Remaining uncredited bytes (e.g., when a file produced 0 live rows).
  uint64_t bytes = 0;
  /// Identity of the completed task, folded into `completed_files_digest_`.
  std::string file_path;
  int64_t start = 0;
  int64_t length = 0;
};

/// Pins the resolved snapshot ID, plan schema ID, and metadata-count mode of a
/// one-shot read for checkpoint resume.
struct SnapshotPinnedMessage {
  Option<int64_t> snapshot_id;
  Option<int32_t> plan_schema_id;
  bool counted_from_metadata = false;
};

/// Marks the start of a new bounded or live window targeting `to_snapshot_id`.
struct WindowStartedMessage {
  int64_t to_snapshot_id = 0;
  Option<int32_t> plan_schema_id;
  bool counted_from_metadata = false;
};

/// Marks completion of a live window up to `snapshot_id`.
struct WindowDoneMessage {
  int64_t snapshot_id = 0;
};

/// Non-fatal warning deduplicated on the operator fiber.
struct WarningMessage {
  std::string message;
};

/// Fatal error reported by the background reader coroutine.
struct ErrorMessage {
  std::string message;
  location source;
  std::string hint;
};

/// Signals that the background reader coroutine has exited.
struct DoneMessage {
  bool completed = false;
};

using Message = variant<BatchMessage, FileDoneMessage, SnapshotPinnedMessage,
                        WindowStartedMessage, WindowDoneMessage, WarningMessage,
                        ErrorMessage, DoneMessage>;
using MessageQueue = tenzir::BoundedQueue<Message, true, true>;

/// One converted batch prefetched by a per-file worker coroutine.
struct SlotBatch {
  nova::Events events;
  std::vector<std::string> warnings;
  uint64_t raw_rows = 0;
  uint64_t bytes = 0;
};

/// Signals normal end-of-file for a prefetched file slot.
struct SlotEof {
  uint64_t remaining_bytes = 0;
};

/// Signals an I/O or decode failure for a prefetched file slot.
struct SlotError {
  std::string message;
};

using SlotItem = variant<SlotBatch, SlotEof, SlotError>;

/// Bounded SPSC channel connecting one concurrent file reader worker to the
/// ordered coordinator in `stream_plan`.
struct PrefetchSlot {
  PrefetchSlot(std::string path, int64_t s, int64_t len, uint64_t est)
    : file_path{std::move(path)},
      start{s},
      length{len},
      estimated_rows{est},
      queue{per_file_queue_capacity} {
  }

  std::string file_path;
  int64_t start = 0;
  int64_t length = 0;
  uint64_t estimated_rows = 0;
  tenzir::BoundedQueue<SlotItem, true, true> queue;
};

/// Upper bound on rows yielded by `task`, taken from manifest metadata.
auto estimated_rows(ice::FileScanTask const& task) -> uint64_t {
  auto const& file = task.data_file();
  if (not file or file->record_count <= 0) {
    return 0;
  }
  return static_cast<uint64_t>(file->record_count);
}

/// Shared state coordinating the operator and its background reader task.
struct RuntimeState {
  RuntimeState() : queue{message_queue_capacity} {
  }

  MessageQueue queue;
  folly::CancellationSource cancel_source;

  auto stop() -> void {
    cancel_source.requestCancellation();
  }

  auto is_stopping() const -> bool {
    return cancel_source.isCancellationRequested();
  }
};

class FromIceberg final : public Operator<void, nova::Events> {
public:
  FromIceberg() = default;

  explicit FromIceberg(FromIcebergArgs args)
    : args_{std::move(args)},
      decimal_format_{args_.decimal_format
                        ? parse_decimal_format(args_.decimal_format->inner)
                            .value_or(DecimalFormat::string)
                        : DecimalFormat::string},
      filter_{args_.optimization.filter},
      remaining_{args_.optimization.limit},
      projection_{pushdown::read_projection_paths(args_.optimization.projection,
                                                  filter_)},
      last_snapshot_id_{
        (args_.live and args_.live->inner and args_.from_snapshot_id)
          ? Option<int64_t>{args_.from_snapshot_id->inner}
          : Option<int64_t>{None{}}} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (done_ or remaining_ == uint64_t{0}) {
      done_ = true;
      finished_ = true;
      co_return;
    }
    events_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_iceberg"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    bytes_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_iceberg"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    for (auto const& predicate : filter_) {
      auto evaluator = co_await nova::Evaluator::make(predicate, ctx);
      if (not evaluator) {
        finished_ = true;
        co_return;
      }
      filters_.push_back(std::move(*evaluator));
    }
    auto config = co_await resolve_catalog_config(args_, "from_iceberg", ctx);
    if (not config) {
      finished_ = true;
      co_return;
    }
    auto [ns, name] = split_table_id(args_.table_id.inner);
    auto opened = co_await retry_transient(
      "open catalog", args_.operator_location, ctx.dh(),
      [cfg = std::move(*config)]() -> Result<std::shared_ptr<ice::Catalog>> {
        return open_catalog(cfg);
      });
    if (not opened) {
      finished_ = true;
      co_return;
    }
    catalog_ = std::move(*opened);
    auto loaded = co_await retry_transient(
      fmt::format("load table `{}`", args_.table_id.inner),
      args_.table_id.source, ctx.dh(),
      [catalog = catalog_, ns = std::move(ns),
       name = std::move(name)]() -> Result<std::shared_ptr<ice::Table>> {
        return load_table(*catalog, ns, name);
      });
    if (not loaded) {
      finished_ = true;
      co_return;
    }
    table_ = std::move(*loaded);
    auto const restored = not table_uuid_.empty();
    if (restored and table_->uuid() != table_uuid_) {
      diagnostic::error("Iceberg table `{}` was recreated since the last "
                        "checkpoint (UUID changed from `{}` to `{}`)",
                        args_.table_id.inner, table_uuid_, table_->uuid())
        .primary(args_.table_id.source)
        .emit(ctx.dh());
      finished_ = true;
      co_return;
    }
    table_uuid_ = table_->uuid();
    if (live() and not last_snapshot_id_ and args_.from_snapshot_id) {
      last_snapshot_id_ = args_.from_snapshot_id->inner;
    }
    if (live() and args_.ref) {
      auto is_tag
        = co_await spawn_blocking([table = table_, ref = args_.ref->inner]() {
            return is_tag_ref(*table, ref);
          });
      if (not is_tag) {
        diagnostic::error("{}", is_tag.error().message)
          .primary(args_.ref->source)
          .emit(ctx.dh());
        finished_ = true;
        co_return;
      }
      if (*is_tag) {
        diagnostic::error("cannot use `live=true` with tag `{}`",
                          args_.ref->inner)
          .primary(args_.ref->source)
          .note("tags are immutable snapshot references; use a branch instead")
          .emit(ctx.dh());
        finished_ = true;
        co_return;
      }
    }
    // Capture mutable cursor state by value before spawning the background
    // reader so `run_reader` never reads operator members mutated concurrently
    // by `process_task`.
    auto resume = ResumeCursor{
      .restored = restored,
      .pinned_snapshot_id = pinned_snapshot_id_,
      .last_snapshot_id = last_snapshot_id_,
      .active_to_snapshot_id = active_to_snapshot_id_,
      .file_index = file_index_,
      .rows_in_file = rows_in_file_,
      .current_file_path = current_file_path_,
      .current_file_start = current_file_start_,
      .current_file_length = current_file_length_,
      .completed_files_digest = completed_files_digest_,
      .counted_from_metadata = counted_from_metadata_,
      .remaining = remaining_,
      .plan_schema_id = plan_schema_id_,
    };
    ctx.spawn_task([this, resume = std::move(resume)]() mutable -> Task<void> {
      co_await run_reader(std::move(resume));
    });
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await runtime_->queue.dequeue();
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](BatchMessage msg) -> Task<void> {
        if (done_ or finished_) {
          co_return;
        }
        if (not msg.file_path.empty()) {
          current_file_path_ = std::move(msg.file_path);
          current_file_start_ = msg.start;
          current_file_length_ = msg.length;
        }
        rows_in_file_ += msg.raw_rows;
        if (msg.bytes > 0) {
          bytes_counter_.add(msg.bytes);
        }
        auto events = pushdown::apply_read(std::move(msg.events), filters_,
                                           remaining_, ctx.dh());
        if (auto const rows = events.active_count(); rows > 0) {
          events_counter_.add(rows);
          co_await push(std::move(events));
        }
        if (remaining_ == uint64_t{0}) {
          done_ = true;
          runtime_->stop();
        }
      },
      [&](FileDoneMessage msg) -> Task<void> {
        if (done_ or finished_) {
          co_return;
        }
        if (msg.bytes > 0) {
          bytes_counter_.add(msg.bytes);
        }
        completed_files_digest_ = chain_file_digest(
          completed_files_digest_, msg.file_path, msg.start, msg.length);
        file_index_ += 1;
        rows_in_file_ = 0;
        current_file_path_.clear();
        current_file_start_ = 0;
        current_file_length_ = 0;
        co_return;
      },
      [&](SnapshotPinnedMessage msg) -> Task<void> {
        pinned_snapshot_id_ = msg.snapshot_id;
        plan_schema_id_ = msg.plan_schema_id;
        counted_from_metadata_ = msg.counted_from_metadata;
        co_return;
      },
      [&](WindowStartedMessage msg) -> Task<void> {
        active_to_snapshot_id_ = msg.to_snapshot_id;
        plan_schema_id_ = msg.plan_schema_id;
        counted_from_metadata_ = msg.counted_from_metadata;
        file_index_ = 0;
        rows_in_file_ = 0;
        current_file_path_.clear();
        current_file_start_ = 0;
        current_file_length_ = 0;
        completed_files_digest_.clear();
        co_return;
      },
      [&](WindowDoneMessage msg) -> Task<void> {
        last_snapshot_id_ = msg.snapshot_id;
        active_to_snapshot_id_ = None{};
        plan_schema_id_ = None{};
        counted_from_metadata_ = false;
        file_index_ = 0;
        rows_in_file_ = 0;
        current_file_path_.clear();
        current_file_start_ = 0;
        current_file_length_ = 0;
        completed_files_digest_.clear();
        co_return;
      },
      [&](WarningMessage msg) -> Task<void> {
        warn_once(std::move(msg.message), ctx.dh());
        co_return;
      },
      [&](ErrorMessage msg) -> Task<void> {
        if (not runtime_->is_stopping()) {
          diagnostic::error("{}", msg.message)
            .primary(msg.source)
            .compose([&](auto builder) {
              return msg.hint.empty() ? std::move(builder)
                                      : std::move(builder).hint("{}", msg.hint);
            })
            .emit(ctx.dh());
        }
        finished_ = true;
        runtime_->stop();
        co_return;
      },
      [&](DoneMessage msg) -> Task<void> {
        finished_ = true;
        done_ = done_ or msg.completed;
        co_return;
      });
  }

  auto stop(OpCtx&) -> Task<void> override {
    runtime_->stop();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ or finished_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("table_uuid", table_uuid_);
    serde("done", done_);
    serde("pinned_snapshot_id", pinned_snapshot_id_);
    serde("last_snapshot_id", last_snapshot_id_);
    serde("active_to_snapshot_id", active_to_snapshot_id_);
    serde("file_index", file_index_);
    serde("rows_in_file", rows_in_file_);
    serde("current_file_path", current_file_path_);
    serde("current_file_start", current_file_start_);
    serde("current_file_length", current_file_length_);
    serde("completed_files_digest", completed_files_digest_);
    serde("counted_from_metadata", counted_from_metadata_);
    serde("remaining", remaining_);
    serde("plan_schema_id", plan_schema_id_);
  }

private:
  struct ResumeCursor {
    bool restored = false;
    Option<int64_t> pinned_snapshot_id;
    Option<int64_t> last_snapshot_id;
    Option<int64_t> active_to_snapshot_id;
    uint64_t file_index = 0;
    uint64_t rows_in_file = 0;
    std::string current_file_path;
    int64_t current_file_start = 0;
    int64_t current_file_length = 0;
    std::string completed_files_digest;
    bool counted_from_metadata = false;
    Option<uint64_t> remaining;
    Option<int32_t> plan_schema_id;
  };

  auto live() const -> bool {
    return args_.live and args_.live->inner;
  }

  auto is_incremental_bounded() const -> bool {
    return not live() and (args_.from_snapshot_id or args_.to_snapshot_id);
  }

  auto poll_interval() const -> duration {
    return args_.poll_interval ? args_.poll_interval->inner
                               : duration{default_poll_interval};
  }

  auto schema_name() const -> std::string {
    return fmt::format("iceberg.{}", args_.table_id.inner);
  }

  auto make_scan_options() const -> ScanOptions {
    return ScanOptions{
      .projection = projection_,
      .filter = filter_,
      .count_from_metadata = false,
      .schema_id = None{},
    };
  }

  auto make_read_options() const -> ArrowReadOptions {
    return ArrowReadOptions{
      .decimal_format = decimal_format_,
      .schema_name = schema_name(),
    };
  }

  auto warn_once(std::string message, diagnostic_handler& dh) -> void {
    if (warned_.insert(message).second) {
      diagnostic::warning("{}", message)
        .primary(args_.operator_location)
        .emit(dh);
    }
  }

  auto merged_cancel_token() const -> Task<folly::CancellationToken> {
    co_return folly::cancellation_token_merge(
      co_await folly::coro::co_current_cancellation_token,
      runtime_->cancel_source.getToken());
  }

  auto is_cancelled() const -> Task<bool> {
    if (runtime_->is_stopping()) {
      co_return true;
    }
    auto const token = co_await folly::coro::co_current_cancellation_token;
    co_return token.isCancellationRequested();
  }

  auto sleep_cancelable(duration d) const -> Task<bool> {
    if (co_await is_cancelled()) {
      co_return false;
    }
    auto token = co_await merged_cancel_token();
    auto slept = co_await catch_cancellation(folly::coro::co_withCancellation(
      token, folly::coro::sleep(
               std::chrono::duration_cast<std::chrono::microseconds>(d))));
    co_return slept.has_value() and not co_await is_cancelled();
  }

  auto send(Message message) const -> Task<bool> {
    if (co_await is_cancelled()) {
      co_return false;
    }
    auto token = co_await merged_cancel_token();
    auto sent = co_await catch_cancellation(folly::coro::co_withCancellation(
      token, runtime_->queue.enqueue(std::move(message))));
    co_return sent.has_value() and not co_await is_cancelled();
  }

  /// Always delivers the terminal `DoneMessage` on graceful `stop()` so
  /// `await_task()` -> `process_task()` reaches `OperatorState::done`. Bypasses
  /// queue capacity via `force_enqueue` so shutdown never deadlocks even if
  /// the queue is full.
  auto send_done(bool completed) const -> void {
    runtime_->queue.force_enqueue(Message{DoneMessage{.completed = completed}});
  }

  auto send_error(std::string message, location source,
                  std::string hint = {}) const -> Task<void> {
    co_await send(ErrorMessage{
      .message = std::move(message),
      .source = source,
      .hint = std::move(hint),
    });
  }

  /// Executes a blocking Iceberg operation during `start()` with bounded
  /// exponential backoff on transient errors.
  template <class Fn>
  auto retry_transient(std::string_view action, location source,
                       diagnostic_handler& dh, Fn&& fn)
    -> Task<Option<typename std::invoke_result_t<Fn>::value_type>> {
    auto backoff = duration{initial_retry_backoff};
    for (auto attempt = 1; attempt <= max_transient_attempts; ++attempt) {
      auto result = co_await spawn_blocking(fn);
      if (result) {
        co_return std::move(*result);
      }
      auto const& err = result.error();
      if (err.kind == Error::Kind::transient
          and attempt < max_transient_attempts
          and not co_await is_cancelled()) {
        diagnostic::warning(
          "transient error while trying to {} (attempt {}/{}): {}; retrying",
          action, attempt, max_transient_attempts, err.message)
          .primary(source)
          .emit(dh);
        if (not co_await sleep_cancelable(backoff)) {
          co_return None{};
        }
        backoff *= 2;
        continue;
      }
      if (not co_await is_cancelled()) {
        diagnostic::error("failed to {}: {}", action, err.message)
          .primary(source)
          .emit(dh);
      }
      co_return None{};
    }
    co_return None{};
  }

  /// Refreshes table metadata on the background reader task with bounded
  /// exponential backoff on transient errors.
  auto refresh_with_retry() const -> Task<bool> {
    auto backoff = duration{initial_retry_backoff};
    for (auto attempt = 1; attempt <= max_transient_attempts; ++attempt) {
      auto result = co_await spawn_blocking([table = table_]() {
        return refresh_table(*table);
      });
      if (result) {
        co_return true;
      }
      auto const& err = result.error();
      if (err.kind == Error::Kind::transient
          and attempt < max_transient_attempts
          and not co_await is_cancelled()) {
        if (not co_await send(WarningMessage{fmt::format(
              "transient error refreshing Iceberg table `{}` (attempt {}/{}): "
              "{}; retrying",
              args_.table_id.inner, attempt, max_transient_attempts,
              err.message)})) {
          co_return false;
        }
        if (not co_await sleep_cancelable(backoff)) {
          co_return false;
        }
        backoff *= 2;
        continue;
      }
      co_await send_error(fmt::format("failed to refresh Iceberg table `{}`: "
                                      "{}",
                                      args_.table_id.inner, err.message),
                          args_.table_id.source);
      co_return false;
    }
    co_return false;
  }

  /// Prefetches all batches of one `FileScanTask` into `slot->queue`.
  /// Always closes `TaskBatchReader` on the blocking pool before returning so
  /// file handles never outlive the worker or `ScanPlan`. Catches all
  /// non-cancellation exceptions per file and enqueues them as `SlotError` so
  /// they are reported strictly in file order without cancelling earlier files.
  auto prefetch_file(std::shared_ptr<ScanPlan const> plan,
                     std::shared_ptr<ice::FileScanTask> task,
                     ArrowReadOptions read_opts, Arc<PrefetchSlot> slot) const
    -> Task<void> {
    auto const token = co_await merged_cancel_token();
    if (token.isCancellationRequested()) {
      co_return;
    }
    auto open_res = co_await async_result(spawn_blocking(
      [plan, task, read_opts]() -> Result<std::shared_ptr<TaskBatchReader>> {
        TRY(auto reader, TaskBatchReader::open(*plan, *task, read_opts));
        return std::make_shared<TaskBatchReader>(std::move(reader));
      }));
    if (open_res.is_cancelled()) {
      co_return;
    }
    if (open_res.is_exception()) {
      if (not token.isCancellationRequested()) {
        slot->queue.force_enqueue(SlotItem{SlotError{
          .message = fmt::format("failed to read Iceberg data file `{}`: {}",
                                 slot->file_path,
                                 open_res.exception().what().toStdString()),
        }});
      }
      co_return;
    }
    auto opened = std::move(open_res).value();
    if (not opened) {
      if (not token.isCancellationRequested()) {
        slot->queue.force_enqueue(SlotItem{SlotError{
          .message = std::move(opened).error().message,
        }});
      }
      co_return;
    }
    auto reader = std::move(*opened);
    auto close_reader = [&]() -> Task<void> {
      if (reader) {
        co_await folly::coro::co_withCancellation(
          {}, spawn_blocking([r = std::move(reader)]() mutable {
            r.reset();
          }));
      }
    };
    auto pending_bytes = reader->file_size_bytes();
    while (not token.isCancellationRequested()) {
      auto next_res = co_await async_result(spawn_blocking([reader]() {
        return reader->next();
      }));
      if (next_res.is_cancelled()) {
        co_await close_reader();
        co_return;
      }
      if (next_res.is_exception()) {
        co_await close_reader();
        if (not token.isCancellationRequested()) {
          slot->queue.force_enqueue(SlotItem{SlotError{
            .message = fmt::format("failed to read Iceberg data file `{}`: {}",
                                   slot->file_path,
                                   next_res.exception().what().toStdString()),
          }});
        }
        co_return;
      }
      auto next_batch = std::move(next_res).value();
      if (not next_batch) {
        co_await close_reader();
        if (not token.isCancellationRequested()) {
          slot->queue.force_enqueue(SlotItem{SlotError{
            .message = std::move(next_batch).error().message,
          }});
        }
        co_return;
      }
      if (not *next_batch) {
        co_await close_reader();
        if (not token.isCancellationRequested()) {
          slot->queue.force_enqueue(SlotItem{SlotEof{
            .remaining_bytes = pending_bytes,
          }});
        }
        co_return;
      }
      auto batch = std::move(**next_batch);
      auto const raw_rows = static_cast<uint64_t>(batch.events.active_count());
      if (raw_rows == 0 and batch.warnings.empty()) {
        continue;
      }
      auto const credit_bytes = raw_rows > 0
                                  ? std::exchange(pending_bytes, uint64_t{0})
                                  : uint64_t{0};
      auto enqueued
        = co_await catch_cancellation(folly::coro::co_withCancellation(
          token, slot->queue.enqueue(SlotItem{SlotBatch{
                   .events = std::move(batch.events),
                   .warnings = std::move(batch.warnings),
                   .raw_rows = raw_rows,
                   .bytes = credit_bytes,
                 }})));
      if (not enqueued.has_value()) {
        co_await close_reader();
        co_return;
      }
    }
    co_await close_reader();
  }

  /// Streams all tasks in `plan` in exact deterministic planning order using
  /// an ordered sliding window of up to `max_concurrent_files` concurrent
  /// prefetch workers. Skips the first `skip_files` tasks (after verifying
  /// their chained `(file_path, start, length)` digest against
  /// `expected_completed_digest`) and `skip_rows` raw rows of the first
  /// unskipped task when resuming from a checkpoint. Returns `true` if the
  /// entire plan completed cleanly.
  auto stream_plan(std::shared_ptr<ScanPlan> plan, uint64_t skip_files,
                   uint64_t skip_rows, std::string_view expected_file_path,
                   int64_t expected_file_start, int64_t expected_file_length,
                   std::string_view expected_completed_digest,
                   Option<uint64_t>& budget) const -> Task<bool> {
    auto const row_limit = budget;
    auto const read_opts = make_read_options();
    auto close_plan = [&]() -> Task<void> {
      if (plan) {
        co_await folly::coro::co_withCancellation(
          {}, spawn_blocking([p = std::move(plan)]() mutable {
            p.reset();
          }));
      }
    };
    // Phase 1: skip the checkpointed prefix of already-completed tasks and
    // verify its digest before spawning any prefetch workers.
    auto file_idx = uint64_t{0};
    auto skipped_digest = std::string{};
    while (file_idx < skip_files) {
      if (co_await is_cancelled()) {
        co_await close_plan();
        co_return false;
      }
      auto advanced = co_await async_result(spawn_blocking([plan]() {
        return plan->next_task();
      }));
      if (advanced.is_cancelled()) {
        co_await close_plan();
        co_return false;
      }
      if (advanced.is_exception()) {
        co_await send_error(
          fmt::format("failed to plan Iceberg scan for `{}`: {}",
                      args_.table_id.inner,
                      advanced.exception().what().toStdString()),
          args_.operator_location);
        co_await close_plan();
        co_return false;
      }
      auto next_task = std::move(advanced).value();
      if (not next_task) {
        co_await send_error(
          fmt::format("failed to plan Iceberg scan for `{}`: {}",
                      args_.table_id.inner, next_task.error().message),
          args_.operator_location);
        co_await close_plan();
        co_return false;
      }
      if (not *next_task) {
        co_await send_error(
          fmt::format("cannot resume Iceberg scan for `{}`: planned scan "
                      "yielded only {} file(s), fewer than checkpointed "
                      "file index {}",
                      args_.table_id.inner, file_idx, skip_files),
          args_.operator_location);
        co_await close_plan();
        co_return false;
      }
      auto const& task = **next_task;
      auto const file_path
        = task->data_file() ? task->data_file()->file_path : std::string{};
      skipped_digest = chain_file_digest(skipped_digest, file_path,
                                         task->start(), task->length());
      file_idx += 1;
    }
    if (skip_files > 0 and skipped_digest != expected_completed_digest) {
      co_await send_error(
        fmt::format("cannot resume Iceberg scan for `{}`: first {} "
                    "planned file(s) do not match checkpointed files",
                    args_.table_id.inner, skip_files),
        args_.operator_location);
      co_await close_plan();
      co_return false;
    }
    // Phase 2: prefetch up to `window` files concurrently while draining their
    // queues strictly in planning order so output order and checkpoint state
    // stay 100% deterministic.
    // iceberg-cpp builds `NameMapping` lookup tables lazily without locking, so
    // fall back to a window of 1 when the table defines a default name mapping.
    auto const window = plan->name_mapping ? size_t{1} : max_concurrent_files;
    auto const exact_rows = filter_.empty();
    auto completed = false;
    auto failed = false;
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      // Always cancel any remaining prefetch workers before `async_scope` joins
      // them on early exit (error, graceful stop, or limit satisfied).
      auto cancel_guard = detail::scope_guard{[&]() noexcept {
        scope.cancel();
      }};
      auto slots = std::deque<Arc<PrefetchSlot>>{};
      auto exhausted = false;
      auto plan_error = Option<std::string>{None{}};
      auto files_done = size_t{0};
      auto delivered_rows = uint64_t{0};
      auto reserved_rows = uint64_t{0};
      auto first_unskipped = true;
      auto refill = [&]() -> Task<bool> {
        // With a limit behind a local filter, ramp up the window as files
        // complete without satisfying the limit.
        auto const allowed
          = (row_limit and not exact_rows)
              ? std::min(window, size_t{1} + size_t{2} * files_done)
              : window;
        while (not exhausted and slots.size() < allowed) {
          // With an unfiltered limit, avoid opening more files than needed to
          // cover `*row_limit` rows; if deletes leave fewer live rows, the next
          // iteration opens another file after `reserved_rows` drops.
          if (row_limit and exact_rows
              and (delivered_rows >= *row_limit
                   or reserved_rows >= *row_limit - delivered_rows)) {
            break;
          }
          if (co_await is_cancelled()) {
            break;
          }
          // Defer planning errors until all already-prefetched earlier files
          // drain in order, so a later manifest failure neither drops earlier
          // rows nor breaks a `head` limit already satisfied by earlier files.
          auto advanced = co_await async_result(spawn_blocking([plan]() {
            return plan->next_task();
          }));
          if (advanced.is_cancelled()) {
            break;
          }
          if (advanced.is_exception()) {
            plan_error = fmt::format("failed to plan Iceberg scan for `{}`: {}",
                                     args_.table_id.inner,
                                     advanced.exception().what().toStdString());
            exhausted = true;
            break;
          }
          auto next_task = std::move(advanced).value();
          if (not next_task) {
            plan_error
              = fmt::format("failed to plan Iceberg scan for `{}`: {}",
                            args_.table_id.inner, next_task.error().message);
            exhausted = true;
            break;
          }
          if (not *next_task) {
            if (first_unskipped and skip_rows > 0) {
              co_await send_error(
                fmt::format("cannot resume Iceberg scan mid-file for `{}`: "
                            "checkpointed file `{}` [{}, +{}) is missing from "
                            "the planned scan",
                            args_.table_id.inner, expected_file_path,
                            expected_file_start, expected_file_length),
                args_.operator_location);
              co_return false;
            }
            exhausted = true;
            break;
          }
          auto task = std::move(**next_task);
          auto const file_path
            = task->data_file() ? task->data_file()->file_path : std::string{};
          auto const task_start = task->start();
          auto const task_length = task->length();
          auto const is_first_unskipped = std::exchange(first_unskipped, false);
          if (is_first_unskipped and skip_rows > 0
              and not expected_file_path.empty()
              and (file_path != expected_file_path
                   or task_start != expected_file_start
                   or task_length != expected_file_length)) {
            co_await send_error(
              fmt::format("cannot resume Iceberg scan mid-file for `{}`: task "
                          "{} resolved to `{}` [{}, +{}) instead of "
                          "checkpointed file `{}` [{}, +{})",
                          args_.table_id.inner, file_idx, file_path, task_start,
                          task_length, expected_file_path, expected_file_start,
                          expected_file_length),
              args_.operator_location);
            co_return false;
          }
          file_idx += 1;
          auto est = estimated_rows(*task);
          if (is_first_unskipped) {
            est = est > skip_rows ? est - skip_rows : uint64_t{0};
          }
          reserved_rows += est;
          auto slot = Arc<PrefetchSlot>{std::in_place, file_path, task_start,
                                        task_length, est};
          slots.push_back(slot);
          scope.spawn([this, plan, task = std::move(task), read_opts,
                       slot = std::move(slot)]() mutable -> Task<void> {
            auto result = co_await async_result(
              prefetch_file(std::move(plan), std::move(task), read_opts, slot));
            if (result.is_exception()) {
              auto const token = co_await merged_cancel_token();
              if (not token.isCancellationRequested()) {
                slot->queue.force_enqueue(SlotItem{SlotError{
                  .message = fmt::format(
                    "failed to read Iceberg data file `{}`: {}",
                    slot->file_path, result.exception().what().toStdString()),
                }});
              }
            }
          });
        }
        co_return true;
      };
      auto first_slot = true;
      while (true) {
        if (not co_await refill()) {
          failed = true;
          break;
        }
        if (slots.empty()) {
          if (plan_error
              and not(row_limit and exact_rows and delivered_rows >= *row_limit)
              and not co_await is_cancelled()) {
            co_await send_error(*plan_error, args_.operator_location);
            failed = true;
          }
          completed = exhausted and not plan_error;
          break;
        }
        auto slot = std::move(slots.front());
        slots.pop_front();
        auto slot_remaining_skip
          = std::exchange(first_slot, false) ? skip_rows : uint64_t{0};
        auto slot_pending_bytes = uint64_t{0};
        auto first_batch = true;
        auto slot_done = false;
        auto const dequeue_token = co_await merged_cancel_token();
        while (not slot_done) {
          auto item
            = co_await catch_cancellation(folly::coro::co_withCancellation(
              dequeue_token, slot->queue.dequeue()));
          if (not item.has_value()) {
            break;
          }
          auto const keep_going = co_await co_match(
            std::move(*item),
            [&](SlotBatch batch) -> Task<bool> {
              slot_pending_bytes += batch.bytes;
              for (auto& warning : batch.warnings) {
                if (not co_await send(WarningMessage{std::move(warning)})) {
                  co_return false;
                }
              }
              if (slot_remaining_skip > 0) {
                batch.events = pushdown::skip_rows(std::move(batch.events),
                                                   slot_remaining_skip);
              }
              auto const unskipped_rows
                = static_cast<uint64_t>(batch.events.active_count());
              if (unskipped_rows == 0) {
                co_return true;
              }
              auto const credit_bytes
                = std::exchange(slot_pending_bytes, uint64_t{0});
              auto batch_file_path
                = first_batch ? slot->file_path : std::string{};
              first_batch = false;
              if (not co_await send(BatchMessage{
                    .events = std::move(batch.events),
                    .raw_rows = unskipped_rows,
                    .bytes = credit_bytes,
                    .file_path = std::move(batch_file_path),
                    .start = slot->start,
                    .length = slot->length,
                  })) {
                co_return false;
              }
              delivered_rows += unskipped_rows;
              if (budget and exact_rows) {
                *budget -= std::min(*budget, unskipped_rows);
              }
              co_return true;
            },
            [&](SlotEof eof) -> Task<bool> {
              slot_done = true;
              if (slot_remaining_skip > 0) {
                failed = true;
                co_await send_error(
                  fmt::format("cannot resume Iceberg scan mid-file for `{}`: "
                              "data file `{}` ended {} row(s) before the "
                              "checkpointed offset",
                              args_.table_id.inner, slot->file_path,
                              slot_remaining_skip),
                  args_.operator_location);
                co_return false;
              }
              slot_pending_bytes += eof.remaining_bytes;
              co_return co_await send(FileDoneMessage{
                .bytes = slot_pending_bytes,
                .file_path = slot->file_path,
                .start = slot->start,
                .length = slot->length,
              });
            },
            [&](SlotError err) -> Task<bool> {
              slot_done = true;
              failed = true;
              co_await send_error(std::move(err.message),
                                  args_.operator_location);
              co_return false;
            });
          if (not keep_going) {
            break;
          }
        }
        if (failed or not slot_done or co_await is_cancelled()) {
          break;
        }
        reserved_rows -= slot->estimated_rows;
        files_done += 1;
      }
    });
    co_await close_plan();
    co_return completed and not failed and not co_await is_cancelled();
  }

  auto warn_for_window(IncrementalWindowSummary const& summary) const
    -> Task<bool> {
    if (summary.overwrite_snapshots > 0 or summary.delete_snapshots > 0) {
      auto msg = fmt::format("incremental scan of `{}` skipped {} overwrite "
                             "and {} delete "
                             "snapshot(s); only appended rows are emitted",
                             args_.table_id.inner, summary.overwrite_snapshots,
                             summary.delete_snapshots);
      if (not co_await send(WarningMessage{std::move(msg)})) {
        co_return false;
      }
    }
    co_return true;
  }

  auto run_reader(ResumeCursor resume) const -> Task<void> {
    auto completed = false;
    if (live()) {
      completed = co_await run_live(resume);
    } else if (is_incremental_bounded()) {
      completed = co_await run_bounded_incremental(resume);
    } else {
      completed = co_await run_one_shot(resume);
    }
    send_done(completed);
  }

  auto run_one_shot(ResumeCursor const& resume) const -> Task<bool> {
    if (co_await is_cancelled()) {
      co_return false;
    }
    auto const restoring_pinned
      = resume.restored and resume.pinned_snapshot_id.has_value();
    auto target = ScanTarget{};
    if (restoring_pinned) {
      target.snapshot_id = resume.pinned_snapshot_id;
    } else {
      if (args_.snapshot_id) {
        target.snapshot_id = args_.snapshot_id->inner;
      }
      if (args_.as_of) {
        target.as_of_ms = std::chrono::floor<std::chrono::milliseconds>(
                            args_.as_of->inner.time_since_epoch())
                            .count();
      }
      if (args_.ref) {
        target.ref = args_.ref->inner;
      }
    }
    auto const skip_files = resume.restored ? resume.file_index : uint64_t{0};
    auto const skip_rows = resume.restored ? resume.rows_in_file : uint64_t{0};
    auto scan_opts = make_scan_options();
    // Fresh scans may answer unfiltered zero-column queries directly from
    // table metadata; resumed scans request the same layout and plan schema
    // recorded in the checkpoint so task indices, paths, and column names match.
    scan_opts.count_from_metadata
      = restoring_pinned ? resume.counted_from_metadata : true;
    if (restoring_pinned) {
      scan_opts.schema_id = resume.plan_schema_id;
    }
    auto planned = co_await spawn_blocking(
      [table = table_, target = std::move(target),
       scan_opts
       = std::move(scan_opts)]() -> Result<std::shared_ptr<ScanPlan>> {
        TRY(auto plan, plan_snapshot_scan(*table, target, scan_opts));
        return std::make_shared<ScanPlan>(std::move(plan));
      });
    if (not planned) {
      co_await send_error(
        fmt::format("failed to plan Iceberg scan for `{}`: {}",
                    args_.table_id.inner, planned.error().message),
        args_.operator_location);
      co_return false;
    }
    auto plan = std::move(*planned);
    auto close_plan = [&]() -> Task<void> {
      if (plan) {
        co_await folly::coro::co_withCancellation(
          {}, spawn_blocking([p = std::move(plan)]() mutable {
            p.reset();
          }));
      }
    };
    if (restoring_pinned and resume.counted_from_metadata
        and not plan->counted_from_metadata) {
      co_await close_plan();
      co_await send_error(
        fmt::format("cannot resume Iceberg scan for `{}`: checkpointed "
                    "metadata-only row count is unavailable on restore",
                    args_.table_id.inner),
        args_.operator_location);
      co_return false;
    }
    if (not plan->snapshot_id) {
      co_await close_plan();
      co_return true;
    }
    if (not restoring_pinned) {
      auto const plan_schema_id
        = plan->table_schema ? Option<int32_t>{plan->table_schema->schema_id()}
                             : Option<int32_t>{None{}};
      if (not co_await send(SnapshotPinnedMessage{
            .snapshot_id = plan->snapshot_id,
            .plan_schema_id = plan_schema_id,
            .counted_from_metadata = plan->counted_from_metadata,
          })) {
        co_await close_plan();
        co_return false;
      }
    }
    auto const expected_path = resume.restored
                                 ? std::string_view{resume.current_file_path}
                                 : std::string_view{};
    auto const expected_start
      = resume.restored ? resume.current_file_start : int64_t{0};
    auto const expected_length
      = resume.restored ? resume.current_file_length : int64_t{0};
    auto const expected_digest
      = resume.restored ? std::string_view{resume.completed_files_digest}
                        : std::string_view{};
    auto budget = resume.remaining;
    co_return co_await stream_plan(std::move(plan), skip_files, skip_rows,
                                   expected_path, expected_start,
                                   expected_length, expected_digest, budget);
  }

  auto run_bounded_incremental(ResumeCursor const& resume) const -> Task<bool> {
    if (co_await is_cancelled()) {
      co_return false;
    }
    auto branch = args_.ref ? Option<std::string>{args_.ref->inner}
                            : Option<std::string>{None{}};
    auto from_id = args_.from_snapshot_id
                     ? Option<int64_t>{args_.from_snapshot_id->inner}
                     : Option<int64_t>{None{}};
    auto to_id = Option<int64_t>{None{}};
    auto resuming_window = false;
    if (resume.restored and resume.active_to_snapshot_id) {
      to_id = resume.active_to_snapshot_id;
      resuming_window = true;
    } else if (args_.to_snapshot_id) {
      to_id = args_.to_snapshot_id->inner;
    } else {
      auto resolved = co_await spawn_blocking([table = table_, branch]() {
        auto ref = branch ? Option<std::string_view>{std::string_view{*branch}}
                          : Option<std::string_view>{None{}};
        return resolve_current_snapshot(*table, ref);
      });
      if (not resolved) {
        co_await send_error(resolved.error().message, args_.operator_location);
        co_return false;
      }
      to_id = *resolved;
    }
    if (not to_id) {
      if (from_id) {
        co_await send_error(fmt::format("snapshot {} not found: table `{}` has "
                                        "no snapshots",
                                        *from_id, args_.table_id.inner),
                            args_.from_snapshot_id->source);
        co_return false;
      }
      co_return true;
    }
    if (from_id) {
      auto inspected = co_await spawn_blocking(
        [table = table_, from = *from_id, to = *to_id]() {
          return inspect_incremental_window(*table, from, to);
        });
      if (not inspected) {
        // Only a missing end snapshot is reported as not found; every other
        // failure concerns the start snapshot or the history behind it.
        auto const source = inspected.error().kind == Error::Kind::not_found
                                and args_.to_snapshot_id
                              ? args_.to_snapshot_id->source
                              : args_.from_snapshot_id->source;
        co_await send_error(inspected.error().message, source);
        co_return false;
      }
      if (not co_await warn_for_window(*inspected)) {
        co_return false;
      }
    }
    auto range = IncrementalRange{
      .from_snapshot_id = from_id,
      .to_snapshot_id = to_id,
      .branch = branch,
    };
    auto const skip_files = resuming_window ? resume.file_index : uint64_t{0};
    auto const skip_rows = resuming_window ? resume.rows_in_file : uint64_t{0};
    auto scan_opts = make_scan_options();
    if (resuming_window) {
      scan_opts.schema_id = resume.plan_schema_id;
    }
    auto planned = co_await spawn_blocking(
      [table = table_, range = std::move(range),
       scan_opts
       = std::move(scan_opts)]() -> Result<std::shared_ptr<ScanPlan>> {
        TRY(auto plan, plan_incremental_append_scan(*table, range, scan_opts));
        return std::make_shared<ScanPlan>(std::move(plan));
      });
    if (not planned) {
      co_await send_error(
        fmt::format("failed to plan incremental Iceberg scan for `{}`: {}",
                    args_.table_id.inner, planned.error().message),
        args_.operator_location);
      co_return false;
    }
    auto plan = std::move(*planned);
    auto close_plan = [&]() -> Task<void> {
      if (plan) {
        co_await folly::coro::co_withCancellation(
          {}, spawn_blocking([p = std::move(plan)]() mutable {
            p.reset();
          }));
      }
    };
    if (resuming_window and resume.counted_from_metadata
        and not plan->counted_from_metadata) {
      co_await close_plan();
      co_await send_error(
        fmt::format("cannot resume Iceberg scan for `{}`: checkpointed "
                    "metadata-only row count is unavailable on restore",
                    args_.table_id.inner),
        args_.operator_location);
      co_return false;
    }
    if (not resuming_window) {
      auto const plan_schema_id
        = plan->table_schema ? Option<int32_t>{plan->table_schema->schema_id()}
                             : Option<int32_t>{None{}};
      if (not co_await send(WindowStartedMessage{
            .to_snapshot_id = *to_id,
            .plan_schema_id = plan_schema_id,
            .counted_from_metadata = false,
          })) {
        co_await close_plan();
        co_return false;
      }
    }
    auto const expected_path = resuming_window
                                 ? std::string_view{resume.current_file_path}
                                 : std::string_view{};
    auto const expected_start
      = resuming_window ? resume.current_file_start : int64_t{0};
    auto const expected_length
      = resuming_window ? resume.current_file_length : int64_t{0};
    auto const expected_digest
      = resuming_window ? std::string_view{resume.completed_files_digest}
                        : std::string_view{};
    auto budget = resume.remaining;
    co_return co_await stream_plan(std::move(plan), skip_files, skip_rows,
                                   expected_path, expected_start,
                                   expected_length, expected_digest, budget);
  }

  auto run_live(ResumeCursor const& resume) const -> Task<bool> {
    if (co_await is_cancelled()) {
      co_return false;
    }
    auto branch = args_.ref ? Option<std::string>{args_.ref->inner}
                            : Option<std::string>{None{}};
    auto last_snapshot = resume.last_snapshot_id;
    // Track the remaining unfiltered limit across windows so `live=true | head`
    // never over-prefetches in later windows.
    auto live_remaining = resume.remaining;
    // Helper to execute one window (either initial snapshot or incremental
    // range `(last_snapshot, target_id]`).
    auto execute_window
      = [&](int64_t target_id, bool resuming_window, uint64_t skip_files,
            uint64_t skip_rows, std::string_view expected_file_path,
            int64_t expected_file_start, int64_t expected_file_length,
            std::string_view expected_completed_digest,
            bool resume_counted_from_metadata) -> Task<bool> {
      auto scan_opts = make_scan_options();
      if (resuming_window) {
        scan_opts.schema_id = resume.plan_schema_id;
      }
      auto start_window = [&](std::shared_ptr<ScanPlan>& plan) -> Task<bool> {
        auto close_plan = [&]() -> Task<void> {
          if (plan) {
            co_await folly::coro::co_withCancellation(
              {}, spawn_blocking([p = std::move(plan)]() mutable {
                p.reset();
              }));
          }
        };
        if (resuming_window and resume_counted_from_metadata
            and not plan->counted_from_metadata) {
          co_await close_plan();
          co_await send_error(
            fmt::format("cannot resume Iceberg scan for `{}`: checkpointed "
                        "metadata-only row count is unavailable on restore",
                        args_.table_id.inner),
            args_.operator_location);
          co_return false;
        }
        if (not resuming_window) {
          auto const plan_schema_id
            = plan->table_schema
                ? Option<int32_t>{plan->table_schema->schema_id()}
                : Option<int32_t>{None{}};
          if (not co_await send(WindowStartedMessage{
                .to_snapshot_id = target_id,
                .plan_schema_id = plan_schema_id,
                .counted_from_metadata = plan->counted_from_metadata,
              })) {
            co_await close_plan();
            co_return false;
          }
        }
        co_return true;
      };
      if (not last_snapshot) {
        scan_opts.count_from_metadata
          = resuming_window ? resume_counted_from_metadata : true;
        auto target = ScanTarget{
          .snapshot_id = target_id,
          .as_of_ms = None{},
          .ref = None{},
        };
        auto planned = co_await spawn_blocking(
          [table = table_, target,
           scan_opts
           = std::move(scan_opts)]() -> Result<std::shared_ptr<ScanPlan>> {
            TRY(auto plan, plan_snapshot_scan(*table, target, scan_opts));
            return std::make_shared<ScanPlan>(std::move(plan));
          });
        if (not planned) {
          co_await send_error(
            fmt::format("failed to plan Iceberg snapshot scan for `{}`: {}",
                        args_.table_id.inner, planned.error().message),
            args_.operator_location);
          co_return false;
        }
        auto plan = std::move(*planned);
        if (not co_await start_window(plan)) {
          co_return false;
        }
        if (not co_await stream_plan(
              std::move(plan), skip_files, skip_rows, expected_file_path,
              expected_file_start, expected_file_length,
              expected_completed_digest, live_remaining)) {
          co_return false;
        }
      } else {
        auto inspected = co_await spawn_blocking(
          [table = table_, from = *last_snapshot, to = target_id]() {
            return inspect_incremental_window(*table, from, to);
          });
        if (not inspected) {
          // The history behind the reader was cut, for example because
          // snapshots expired or the branch was rolled back. There is no
          // position to continue from, so the read fails.
          co_await send_error(
            inspected.error().message, args_.operator_location,
            "the live read cannot continue from the last snapshot it read; "
            "a restart without that state reads the whole table again "
            "unless `from_snapshot_id` names a snapshot in the current "
            "history of the table");
          co_return false;
        }
        if (not co_await warn_for_window(*inspected)) {
          co_return false;
        }
        auto range = IncrementalRange{
          .from_snapshot_id = last_snapshot,
          .to_snapshot_id = target_id,
          .branch = branch,
        };
        auto planned = co_await spawn_blocking(
          [table = table_, range = std::move(range),
           scan_opts
           = std::move(scan_opts)]() -> Result<std::shared_ptr<ScanPlan>> {
            TRY(auto plan,
                plan_incremental_append_scan(*table, range, scan_opts));
            return std::make_shared<ScanPlan>(std::move(plan));
          });
        if (not planned) {
          co_await send_error(
            fmt::format("failed to plan incremental Iceberg scan for `{}`: {}",
                        args_.table_id.inner, planned.error().message),
            args_.operator_location);
          co_return false;
        }
        auto plan = std::move(*planned);
        if (not co_await start_window(plan)) {
          co_return false;
        }
        if (not co_await stream_plan(
              std::move(plan), skip_files, skip_rows, expected_file_path,
              expected_file_start, expected_file_length,
              expected_completed_digest, live_remaining)) {
          co_return false;
        }
      }
      last_snapshot = target_id;
      co_return co_await send(WindowDoneMessage{.snapshot_id = target_id});
    };
    // 1. If restored mid-window, complete the interrupted window first.
    if (resume.restored and resume.active_to_snapshot_id) {
      if (not co_await execute_window(
            *resume.active_to_snapshot_id, /*resuming_window=*/true,
            resume.file_index, resume.rows_in_file, resume.current_file_path,
            resume.current_file_start, resume.current_file_length,
            resume.completed_files_digest, resume.counted_from_metadata)) {
        co_return false;
      }
    }
    // 2. Immediately check the current snapshot (both on fresh start and after
    //    restoring) so commits made before startup or while down are picked up
    //    without waiting for the first poll interval.
    auto initial_current = co_await spawn_blocking([table = table_, branch]() {
      auto ref = branch ? Option<std::string_view>{std::string_view{*branch}}
                        : Option<std::string_view>{None{}};
      return resolve_current_snapshot(*table, ref);
    });
    if (not initial_current) {
      co_await send_error(initial_current.error().message,
                          args_.operator_location);
      co_return false;
    }
    if (*initial_current and *initial_current != last_snapshot) {
      if (not co_await execute_window(**initial_current,
                                      /*resuming_window=*/false, 0, 0, {}, 0, 0,
                                      {}, false)) {
        co_return false;
      }
    }
    // 3. Poll loop for new snapshots until stopped or limit reached.
    auto const interval = poll_interval();
    while (not co_await is_cancelled()) {
      if (not co_await sleep_cancelable(interval)) {
        break;
      }
      if (not co_await refresh_with_retry()) {
        break;
      }
      if (table_->uuid() != table_uuid_) {
        co_await send_error(
          fmt::format("Iceberg table `{}` was recreated during live read "
                      "(UUID changed from `{}` to `{}`)",
                      args_.table_id.inner, table_uuid_, table_->uuid()),
          args_.table_id.source);
        co_return false;
      }
      auto current = co_await spawn_blocking([table = table_, branch]() {
        auto ref = branch ? Option<std::string_view>{std::string_view{*branch}}
                          : Option<std::string_view>{None{}};
        return resolve_current_snapshot(*table, ref);
      });
      if (not current) {
        co_await send_error(current.error().message, args_.operator_location);
        co_return false;
      }
      if (not *current or *current == last_snapshot) {
        continue;
      }
      if (not co_await execute_window(**current, /*resuming_window=*/false, 0,
                                      0, {}, 0, 0, {}, false)) {
        break;
      }
    }
    co_return false;
  }

  FromIcebergArgs args_;
  DecimalFormat decimal_format_ = DecimalFormat::string;
  ir::OptimizeFilter filter_;
  Option<uint64_t> remaining_;
  Option<ir::OptimizeProjection> projection_;
  std::vector<nova::Evaluator> filters_;

  std::shared_ptr<ice::Catalog> catalog_;
  std::shared_ptr<ice::Table> table_;
  mutable Arc<RuntimeState> runtime_ = Arc<RuntimeState>{std::in_place};

  // Bounded O(1) checkpointed state.
  std::string table_uuid_;
  bool done_ = false;
  Option<int64_t> pinned_snapshot_id_;
  Option<int64_t> last_snapshot_id_;
  Option<int64_t> active_to_snapshot_id_;
  uint64_t file_index_ = 0;
  uint64_t rows_in_file_ = 0;
  std::string current_file_path_;
  int64_t current_file_start_ = 0;
  int64_t current_file_length_ = 0;
  std::string completed_files_digest_;
  bool counted_from_metadata_ = false;
  /// Schema ID of the plan for the active one-shot read or window, so a
  /// checkpoint restore resolves names, filters, and projections consistently
  /// even across schema evolutions.
  Option<int32_t> plan_schema_id_;

  // Ephemeral runtime state.
  bool finished_ = false;
  std::unordered_set<std::string> warned_;
  MetricsCounter events_counter_;
  MetricsCounter bytes_counter_;
};

class FromIcebergPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_iceberg";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromIcebergArgs, FromIceberg>{};
    auto catalog_args = register_catalog_args(d);
    auto decimal_format_arg
      = d.named("decimal_format", &FromIcebergArgs::decimal_format);
    auto snapshot_id_arg
      = d.named("snapshot_id", &FromIcebergArgs::snapshot_id);
    auto as_of_arg = d.named("as_of", &FromIcebergArgs::as_of);
    auto ref_arg = d.named("ref", &FromIcebergArgs::ref);
    auto live_arg = d.named("live", &FromIcebergArgs::live);
    auto poll_interval_arg
      = d.named("poll_interval", &FromIcebergArgs::poll_interval);
    auto from_snapshot_id_arg
      = d.named("from_snapshot_id", &FromIcebergArgs::from_snapshot_id);
    auto to_snapshot_id_arg
      = d.named("to_snapshot_id", &FromIcebergArgs::to_snapshot_id);
    d.operator_location(&FromIcebergArgs::operator_location);
    d.optimization(&FromIcebergArgs::optimization);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      validate_catalog_args(catalog_args, "from_iceberg", ctx);
      if (auto value = ctx.get(decimal_format_arg);
          value and not parse_decimal_format(value->inner)) {
        diagnostic::error("unsupported `decimal_format` `{}`", value->inner)
          .primary(value->source)
          .note("supported decimal formats are `string` and `float`")
          .emit(ctx);
      }
      auto const snapshot_id = ctx.get(snapshot_id_arg);
      auto const as_of = ctx.get(as_of_arg);
      auto const ref = ctx.get(ref_arg);
      auto const live = ctx.get(live_arg);
      auto const poll_interval = ctx.get(poll_interval_arg);
      auto const from_snapshot_id = ctx.get(from_snapshot_id_arg);
      auto const to_snapshot_id = ctx.get(to_snapshot_id_arg);
      auto const is_live = live and live->inner;
      if (snapshot_id and snapshot_id->inner < 0) {
        diagnostic::error("`snapshot_id` must be non-negative")
          .primary(snapshot_id->source)
          .emit(ctx);
      }
      if (from_snapshot_id and from_snapshot_id->inner < 0) {
        diagnostic::error("`from_snapshot_id` must be non-negative")
          .primary(from_snapshot_id->source)
          .emit(ctx);
      }
      if (to_snapshot_id and to_snapshot_id->inner < 0) {
        diagnostic::error("`to_snapshot_id` must be non-negative")
          .primary(to_snapshot_id->source)
          .emit(ctx);
      }
      if (ref and ref->inner.empty()) {
        diagnostic::error("`ref` must not be empty")
          .primary(ref->source)
          .emit(ctx);
      }
      if (poll_interval) {
        if (not is_live) {
          diagnostic::error("`poll_interval` requires `live=true`")
            .primary(poll_interval->source)
            .emit(ctx);
        }
        if (poll_interval->inner <= duration::zero()) {
          diagnostic::error("`poll_interval` must be positive")
            .primary(poll_interval->source)
            .emit(ctx);
        }
      }
      if (to_snapshot_id and not from_snapshot_id and not is_live
          and not snapshot_id and not as_of) {
        diagnostic::error("`to_snapshot_id` requires `from_snapshot_id`")
          .primary(to_snapshot_id->source)
          .emit(ctx);
      }
      auto check_exclusive
        = [&](location first, std::string_view first_name, location second,
              std::string_view second_name) {
            diagnostic::error("`{}` and `{}` are mutually exclusive",
                              first_name, second_name)
              .primary(first)
              .secondary(second)
              .emit(ctx);
          };
      if (snapshot_id) {
        if (as_of) {
          check_exclusive(snapshot_id->source, "snapshot_id", as_of->source,
                          "as_of");
        }
        if (ref) {
          check_exclusive(snapshot_id->source, "snapshot_id", ref->source,
                          "ref");
        }
        if (is_live) {
          check_exclusive(snapshot_id->source, "snapshot_id", live->source,
                          "live=true");
        }
        if (from_snapshot_id) {
          check_exclusive(snapshot_id->source, "snapshot_id",
                          from_snapshot_id->source, "from_snapshot_id");
        }
        if (to_snapshot_id) {
          check_exclusive(snapshot_id->source, "snapshot_id",
                          to_snapshot_id->source, "to_snapshot_id");
        }
      }
      if (as_of) {
        if (is_live) {
          check_exclusive(as_of->source, "as_of", live->source, "live=true");
        }
        if (from_snapshot_id) {
          check_exclusive(as_of->source, "as_of", from_snapshot_id->source,
                          "from_snapshot_id");
        }
        if (to_snapshot_id) {
          check_exclusive(as_of->source, "as_of", to_snapshot_id->source,
                          "to_snapshot_id");
        }
      }
      if (is_live and to_snapshot_id) {
        check_exclusive(live->source, "live=true", to_snapshot_id->source,
                        "to_snapshot_id");
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::iceberg

TENZIR_REGISTER_PLUGIN(tenzir::plugins::iceberg::FromIcebergPlugin)
