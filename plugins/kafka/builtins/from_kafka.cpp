//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/async_consumer.hpp"
#include "kafka/from_kafka_legacy.hpp"
#include "kafka/librdkafka_utils.hpp"
#include "kafka/message_builder.hpp"
#include "kafka/operator_args.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/aws_iam.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/json_parser.hpp"
#include "tenzir/si_literals.hpp"

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/OperationCancelled.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/ViaIfAsync.h>
#include <folly/executors/GlobalExecutor.h>
#include <librdkafka/rdkafkacpp.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tenzir::plugins::kafka {

namespace {

using namespace std::chrono_literals;
using namespace tenzir::si_literals;
using tenzir::detail::ascii_icase_equal;

/// Diagnostic handler that buffers diagnostics for later retrieval.
/// Unlike `collecting_diagnostic_handler`, this supports draining without
/// consuming the handler, so it can be reused across multiple batches.
class BufferingDiagnosticHandler final : public diagnostic_handler {
public:
  void emit(diagnostic diag) override {
    collected_.push_back(std::move(diag));
  }
  auto drain() -> std::vector<diagnostic> {
    auto result = std::move(collected_);
    collected_.clear();
    return result;
  }
  auto empty() const -> bool {
    return collected_.empty();
  }

private:
  std::vector<diagnostic> collected_;
};

/// Concurrent stage layout (executor domains on the right):
///
///   ┌───────────────────────────────┐
///   │ ×P poll loops (`fetch_loop`)  │                [I/O executor]
///   │   1 source per partition      │
///   └───────────────────────────────┘
///                   │
///                   │ MessageBatch
///                   ▼
///            runtime_.message_queue                  [bounded queue]
///                   │
///                   │ ×N workers
///                   ▼
///   ┌───────────────────────────────────────┐
///   │ build_loop                            │        [CPU executor]
///   │   build_table_slice → TableSliceFrame │
///   └───────────────────────────────────────┘
///                   │
///                   │ TableSliceFrame
///                   ▼
///             runtime_.table_slice_queue             [bounded queue]
///                   │
///                   ▼
///   ┌───────────────────────────────┐
///   │ await_task / process_task     │                [operator]
///   └───────────────────────────────┘
///                   │
///       ordered:   reorder → push in order
///       unordered: push as received
///
/// Invariants:
/// 1. Backpressure is bounded by both `runtime_.message_queue` capacity and
///    `prefetch_bytes` accounting (`runtime_.in_flight_fetch_bytes`).
/// 2. `seq` is assigned in source-arrival order across all partition sources;
///    only ordered mode re-establishes that order before emitting slices.
/// 3. Source polling (`fetch_wait_timeout`) is independent from slice
///    flush latency (`batch_timeout`) to avoid timeout-coupling stalls.
/// 4. Source ingress is typically the bottleneck. Raising worker concurrency
///    helps mostly in `optimization="unordered"` mode but does not remove
///    source-side wait on its own.
/// 5. Per-partition record order is preserved by Kafka and by each single
///    partition source loop, but there is no global cross-partition order.

/// Default delay before flushing a partial batch.
constexpr auto default_batch_timeout = 100ms;

/// Default upper bound for one Kafka notification wait in the source poll loop.
constexpr auto default_fetch_wait_timeout = 1ms;

/// Maximum adaptive wait floor for source poll timeout backoff.
constexpr auto fetch_wait_backoff_floor = 16ms;

/// Number of consecutive idle timeouts before doubling fetch wait.
constexpr auto fetch_wait_backoff_after = 8uz;

/// Stores process-wide `from_kafka` defaults from `kafka.yaml`.
auto source_global_defaults() -> record& {
  static auto defaults = record{};
  return defaults;
}

/// Returns high-throughput defaults for librdkafka consumer queueing/polling.
auto from_kafka_throughput_defaults()
  -> const std::array<std::pair<std::string, std::string>, 5>& {
  using entry = std::pair<std::string, std::string>;
  // Performance note: these defaults intentionally bias towards low broker-side
  // holdback (`fetch.min.bytes=1`, `fetch.wait.max.ms=500`) while increasing
  // client queue depth. Local 10M-message fixture runs showed this combination
  // avoids source stalls better than aggressive "large batch" defaults.
  static const auto defaults = std::array<entry, 5>{
    entry{"fetch.min.bytes", std::to_string(1)},
    entry{"fetch.wait.max.ms", std::to_string(500)},
    entry{"max.partition.fetch.bytes", std::to_string(1_Mi)},
    entry{"queued.min.messages", std::to_string(200_k)},
    entry{"queued.max.messages.kbytes", std::to_string(256_k)},
  };
  return defaults;
}

/// Adds high-throughput defaults unless explicitly configured via `kafka.yaml`.
/// User-supplied `from_kafka options={...}` are still applied afterwards.
auto apply_from_kafka_throughput_defaults(record& config) -> void {
  for (auto const& [key, value] : from_kafka_throughput_defaults()) {
    if (config.contains(key)) {
      continue;
    }
    config[key] = value;
  }
}

/// Parsed arguments for `from_kafka`.
struct FromKafkaArgs {
  /// Kafka topic to consume.
  std::string topic;
  /// Maximum number of messages to emit before stopping.
  std::optional<located<uint64_t>> count;
  /// Stop after all assigned partitions reach EOF.
  std::optional<location> exit;
  /// Starting position to use for partition assignment.
  std::optional<located<data>> offset;
  /// Target number of messages per source batch.
  uint64_t batch_size = 10_k;
  /// Region used for MSK IAM token generation.
  std::optional<located<std::string>> aws_region;
  /// IAM authentication settings for OAUTHBEARER.
  std::optional<located<record>> aws_iam;
  /// Raw librdkafka overrides applied to the consumer config.
  located<record> options;
  /// Controls output ordering guarantees.
  std::string _optimization = "ordered";
  /// Preferred build-stage batch size override.
  uint64_t _worker_batch_size = 0;
  /// Number of concurrent build coroutines.
  uint64_t _worker_concurrency = 0;
  /// Capacity of the queue between source and build stages.
  uint64_t _prefetch_batches = 8;
  /// Byte budget for in-flight source payload.
  uint64_t _prefetch_bytes = 256_Mi;
  /// Maximum wait before flushing a partial source batch.
  duration _batch_timeout = default_batch_timeout;
  /// Initial broker poll wait before adaptive timeout backoff.
  duration _fetch_wait_timeout = default_fetch_wait_timeout;
  /// Whether the operator should treat the input as json and produce events
  /// directly
  bool _json = false;
  /// Whether to enable _raw mode for _json.
  bool _raw = false;
};

/// Enumerates batching behavior for emitting processed Kafka records.
TENZIR_ENUM(OptimizationMode, ordered, unordered);

/// Returns whether opt-in `from_kafka` perf counters should be collected.
auto from_kafka_perf_stats_enabled() -> bool {
  static auto enabled = [] {
    auto value = tenzir::detail::getenv("TENZIR_KAFKA_FROM_PERF_STATS");
    if (not value) {
      return false;
    }
    return *value == "1" or ascii_icase_equal(*value, "true")
           or ascii_icase_equal(*value, "yes")
           or ascii_icase_equal(*value, "on");
  }();
  return enabled;
}

/// Converts any chrono duration to integer nanoseconds.
template <class Duration>
auto as_ns(Duration d) -> uint64_t {
  return static_cast<uint64_t>(
    std::chrono::duration_cast<std::chrono::nanoseconds>(d).count());
}

/// Aggregates opt-in runtime counters for stage-level `from_kafka` analysis.
/// Note: `*_ns` values are cumulative nanoseconds across concurrent coroutines
/// and can exceed end-to-end wall-clock time.
struct FromKafkaPerfCounters {
  /// Number of source-loop polls (`AsyncConsumerQueue::next_batch`) started.
  std::atomic<uint64_t> fetch_next_batch_calls = 0;
  /// Total time spent awaiting `next_batch` in the source loop.
  std::atomic<uint64_t> fetch_next_batch_wait_ns = 0;
  /// Number of empty polls that ended due to timeout.
  std::atomic<uint64_t> fetch_timeouts = 0;
  /// Number of non-empty source batches produced by the poll stage.
  std::atomic<uint64_t> fetched_batches = 0;
  /// Number of Kafka messages received from the poll stage.
  std::atomic<uint64_t> fetched_messages = 0;
  /// Sum of source payload bytes before build-stage parsing.
  std::atomic<uint64_t> fetched_payload_bytes = 0;
  /// Time waiting for prefetch-byte budget before enqueueing source batches.
  std::atomic<uint64_t> prefetch_wait_ns = 0;
  /// Time spent waiting to enqueue into `runtime_.message_queue`.
  std::atomic<uint64_t> fetched_enqueue_wait_ns = 0;
  /// Worker time spent waiting to dequeue from `runtime_.message_queue`.
  std::atomic<uint64_t> build_dequeue_wait_ns = 0;
  /// Worker CPU time spent in `build_table_slice`.
  std::atomic<uint64_t> build_compute_ns = 0;
  /// Worker time spent waiting to enqueue into `runtime_.table_slice_queue`.
  std::atomic<uint64_t> build_enqueue_wait_ns = 0;
  /// Number of batches processed by build workers.
  std::atomic<uint64_t> built_batches = 0;
  /// Number of messages processed by build workers.
  std::atomic<uint64_t> built_messages = 0;
  /// Runner time spent waiting to dequeue from `runtime_.table_slice_queue`.
  std::atomic<uint64_t> runner_dequeue_wait_ns = 0;
  /// Time spent waiting on downstream backpressure in `push(...)`.
  std::atomic<uint64_t> push_wait_ns = 0;
  /// Number of non-empty built batches emitted downstream.
  std::atomic<uint64_t> emitted_batches = 0;
  /// Number of messages emitted downstream.
  std::atomic<uint64_t> emitted_messages = 0;
  /// Number of `table_slice` objects pushed downstream.
  std::atomic<uint64_t> emitted_slices = 0;
  /// Number of EOF partition markers observed.
  std::atomic<uint64_t> eof_events = 0;
  /// Number of fatal-error events observed in poll/build/process stages.
  std::atomic<uint64_t> fatal_errors = 0;
};

/// Picks a bounded worker count for the CPU-side builder stage.
auto default_worker_concurrency() -> uint64_t {
  auto hw = static_cast<uint64_t>(std::thread::hardware_concurrency());
  if (hw == 0) {
    hw = 1;
  }
  return std::min<uint64_t>(hw, 8);
}

/// Parses `offset=` values, including symbolic names and tail offsets.
auto parse_offset_value(located<data> const& input, int64_t& offset) -> bool {
  return match(
    input.inner,
    [&](std::string const& value) -> bool {
      return offset_parser()(value, offset);
    },
    [&](int64_t value) -> bool {
      if (value >= 0) {
        offset = value;
      } else {
        offset = RdKafka::Consumer::OffsetTail(-value);
      }
      return true;
    },
    [&](uint64_t value) -> bool {
      constexpr auto max = std::numeric_limits<int64_t>::max();
      if (value > static_cast<uint64_t>(max)) {
        return false;
      }
      offset = static_cast<int64_t>(value);
      return true;
    },
    [&](auto const&) -> bool {
      return false;
    });
}

/// Represents one polled Kafka message batch plus control metadata.
struct MessageBatch {
  uint64_t seq = 0;
  std::vector<AsyncConsumerQueue::Message> messages;
  std::vector<int32_t> eof_partitions;
  std::optional<std::string> fatal_error;
  bool reached_count = false;
  size_t payload_bytes = 0;
};

/// Represents one built table-slice frame plus commit metadata.
struct TableSliceFrame {
  uint64_t seq = 0;
  std::vector<table_slice> slices;
  std::unordered_map<int32_t, int64_t> max_offsets;
  size_t message_count = 0;
  std::vector<int32_t> eof_partitions;
  std::optional<std::string> fatal_error;
  std::vector<diagnostic> diagnostics;
};

/// Result wrapper returned by `await_task()` to `process_task()`.
struct TableSliceResult {
  std::optional<TableSliceFrame> frame;
  bool end_of_stream = false;
};

/// Converts one message batch into a `TableSliceFrame`.
///
/// The `process_payload` callback receives each message's raw payload as a
/// `std::span<const std::byte>` and feeds it into the appropriate builder.
///
/// When `flush` is true, all buffered data is finalized into slices (used in
/// ordered mode to preserve per-batch ordering). Otherwise, only data that
/// exceeds the builder's internal thresholds is yielded (used in unordered
/// mode for higher throughput).
template <typename F>
auto build_table_slice(MessageBatch& batch, multi_series_builder& builder,
                       bool flush, F&& process_payload) -> TableSliceFrame {
  auto frame = TableSliceFrame{};
  frame.seq = batch.seq;
  frame.eof_partitions = std::move(batch.eof_partitions);
  frame.fatal_error = std::move(batch.fatal_error);
  for (auto& message : batch.messages) {
    auto payload_bytes = message.payload();
    if (payload_bytes.is_err()) {
      frame.fatal_error = fmt::format("invalid kafka payload in partition {} "
                                      "at offset {}: {}",
                                      message.partition(), message.offset(),
                                      std::move(payload_bytes).unwrap_err());
      break;
    }
    auto payload = std::move(payload_bytes).unwrap();
    process_payload(payload);
    frame.max_offsets[message.partition()] = message.offset();
    ++frame.message_count;
  }
  if (flush) {
    frame.slices = builder.finalize_as_table_slice();
  } else {
    auto ready = builder.yield_ready_as_table_slice();
    frame.slices = std::move(ready.slices);
  }
  return frame;
}

/// Streaming source operator that consumes Kafka records asynchronously.
class FromKafka final : public Operator<void, table_slice> {
public:
  explicit FromKafka(FromKafkaArgs args) : args_{std::move(args)} {
    perf_enabled_ = from_kafka_perf_stats_enabled();
  }
  FromKafka(FromKafka&& other) noexcept
    : args_{std::move(other.args_)},
      optimization_mode_{other.optimization_mode_},
      worker_count_{other.worker_count_},
      worker_batch_size_{other.worker_batch_size_},
      emitted_messages_{other.emitted_messages_},
      checkpoint_pending_offsets_{std::move(other.checkpoint_pending_offsets_)},
      assigned_partitions_{std::move(other.assigned_partitions_)},
      eof_partitions_{std::move(other.eof_partitions_)},
      perf_enabled_{other.perf_enabled_},
      perf_started_{other.perf_started_},
      perf_start_{other.perf_start_},
      done_{other.done_} {
    runtime_.partition_sources = std::move(other.runtime_.partition_sources);
    runtime_.partition_source_by_id
      = std::move(other.runtime_.partition_source_by_id);
    runtime_.message_queue = std::move(other.runtime_.message_queue);
    runtime_.table_slice_queue = std::move(other.runtime_.table_slice_queue);
    runtime_.ordered_slices = std::move(other.runtime_.ordered_slices);
    runtime_.builders_finished = other.runtime_.builders_finished;
    runtime_.message_queue_closed.store(
      other.runtime_.message_queue_closed.load());
    runtime_.live_fetchers.store(other.runtime_.live_fetchers.load());
    runtime_.live_builders.store(other.runtime_.live_builders.load());
    runtime_.pipeline_stop_requested.store(
      other.runtime_.pipeline_stop_requested.load());
    runtime_.in_flight_fetch_bytes = other.runtime_.in_flight_fetch_bytes;
    runtime_.next_fetch_seq.store(other.runtime_.next_fetch_seq.load());
    runtime_.next_emit_seq = other.runtime_.next_emit_seq;
    runtime_.scheduled_messages.store(other.runtime_.scheduled_messages.load());
  }
  auto operator=(FromKafka&&) -> FromKafka& = delete;
  FromKafka(FromKafka const&) = delete;
  auto operator=(FromKafka const&) -> FromKafka& = delete;

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    if (done_) {
      co_return;
    }
    initialize_perf_tracking();
    auto auth = co_await resolve_aws_iam_auth(
      args_.aws_iam, args_.aws_region, ctx,
      AwsIamRegionRequirement::required_with_iam);
    if (not auth) {
      done_ = true;
      co_return;
    }
    auto offset = resolve_start_offset(ctx);
    if (not offset) {
      done_ = true;
      co_return;
    }
    if (not co_await make_partition_sources(ctx, *auth, *offset)) {
      done_ = true;
      co_return;
    }
    initialize_runtime_state();
    for (auto source_index = size_t{0};
         source_index < runtime_.partition_sources.size(); ++source_index) {
      ctx.spawn_task(folly::coro::co_withExecutor(ctx.io_executor(),
                                                  fetch_loop(source_index)));
    }
    for (size_t i = 0; i < worker_count_; ++i) {
      ctx.spawn_task(build_loop());
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (not runtime_.table_slice_queue) {
      co_return TableSliceResult{.end_of_stream = true};
    }
    try {
      auto token = co_await folly::coro::co_current_cancellation_token;
      if (token.isCancellationRequested()) {
        request_pipeline_stop();
        co_return TableSliceResult{.end_of_stream = true};
      }
      if (optimization_mode_ == OptimizationMode::ordered) {
        co_return co_await await_ordered_batch();
      }
      auto dequeue_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        dequeue_started = std::chrono::steady_clock::now();
      }
      auto next = co_await runtime_.table_slice_queue->dequeue();
      if (perf_enabled_) {
        add_perf_counter(
          perf_.runner_dequeue_wait_ns,
          as_ns(std::chrono::steady_clock::now() - dequeue_started));
      }
      if (not next) {
        emit_perf_summary("end_of_stream");
        co_return TableSliceResult{.end_of_stream = true};
      }
      co_return TableSliceResult{
        .frame = std::move(*next),
      };
    } catch (folly::OperationCancelled const&) {
      request_pipeline_stop();
      emit_perf_summary("cancelled");
      co_return TableSliceResult{.end_of_stream = true};
    }
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto task_result = std::move(result).as<TableSliceResult>();
    if (task_result.end_of_stream) {
      done_ = true;
      emit_perf_summary("end_of_stream");
      request_pipeline_stop();
      co_return;
    }
    TENZIR_ASSERT(task_result.frame);
    auto& frame = *task_result.frame;
    for (auto& d : frame.diagnostics) {
      std::move(d).modify().emit(ctx);
    }
    for (auto& slice : frame.slices) {
      auto push_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        push_started = std::chrono::steady_clock::now();
      }
      co_await push(std::move(slice));
      if (perf_enabled_) {
        add_perf_counter(
          perf_.push_wait_ns,
          as_ns(std::chrono::steady_clock::now() - push_started));
        add_perf_counter(perf_.emitted_slices);
      }
    }
    if (frame.message_count > 0) {
      advance_checkpoint(frame.max_offsets);
      co_await commit_pending_offsets(&ctx.dh());
      emitted_messages_ += frame.message_count;
      add_perf_counter(perf_.emitted_batches);
      add_perf_counter(perf_.emitted_messages,
                       static_cast<uint64_t>(frame.message_count));
    }
    for (auto partition : frame.eof_partitions) {
      add_perf_counter(perf_.eof_events);
      mark_partition_eof(partition);
    }
    if (frame.fatal_error) {
      diagnostic::error("{}", *frame.fatal_error).emit(ctx);
      done_ = true;
      add_perf_counter(perf_.fatal_errors);
      emit_perf_summary("done");
      request_pipeline_stop();
    }
    if (args_.count and emitted_messages_ >= args_.count->inner) {
      // Don't set done_ here; let the pipeline drain so the build_loop
      // can finalize and flush any buffered data from the builder.
      request_pipeline_stop();
    }
  }

  auto post_commit(OpCtx&) -> Task<void> override {
    co_await commit_pending_offsets();
  }

  auto commit_pending_offsets(diagnostic_handler* dh = nullptr) -> Task<void> {
    if (checkpoint_pending_offsets_.empty()) {
      co_return;
    }
    auto offsets_by_source
      = std::unordered_map<size_t, std::vector<RdKafka::TopicPartition*>>{};
    for (auto const& [partition, offset] : checkpoint_pending_offsets_) {
      auto source_it = runtime_.partition_source_by_id.find(partition);
      if (source_it == runtime_.partition_source_by_id.end()) {
        if (dh) {
          diagnostic::warning("from_kafka: no source for partition {} while "
                              "committing",
                              partition)
            .hint("this typically means checkpointed offsets refer to a "
                  "partition that is no longer assigned (for example after a "
                  "topic partition-count change or stale recovery state)")
            .emit(*dh);
        } else {
          TENZIR_WARN("from_kafka: no source for partition {} while committing",
                      partition);
        }
        continue;
      }
      offsets_by_source[source_it->second].push_back(
        RdKafka::TopicPartition::create(args_.topic, partition, offset));
    }
    for (auto& [source_index, offsets] : offsets_by_source) {
      auto cleanup_offsets = tenzir::detail::scope_guard{[&offsets]() noexcept {
        RdKafka::TopicPartition::destroy(offsets);
      }};
      if (offsets.empty()
          or source_index >= runtime_.partition_sources.size()) {
        continue;
      }
      auto& source = runtime_.partition_sources[source_index];
      if (not source.source_consumer) {
        continue;
      }
      if (auto err = source.source_consumer->consumer->commitSync(offsets);
          err != RdKafka::ERR_NO_ERROR) {
        if (dh) {
          diagnostic::warning("from_kafka: failed to commit offsets for "
                              "partition {}",
                              source.partition)
            .note("librdkafka error: {}", RdKafka::err2str(err))
            .emit(*dh);
        } else {
          TENZIR_WARN("from_kafka: failed to commit offsets for partition {}: "
                      "{}",
                      source.partition, RdKafka::err2str(err));
        }
      }
    }
    checkpoint_pending_offsets_.clear();
  }

  auto snapshot(Serde& serde) -> void override {
    // Persist only recovery state that reflects consumed progress.
    serde("emitted_messages", emitted_messages_);
    serde("checkpoint_pending_offsets", checkpoint_pending_offsets_);
  }

  /// Stops background tasks as soon as the runner asks this source to stop.
  auto stop(OpCtx&) -> Task<void> override {
    request_pipeline_stop();
    emit_perf_summary("stop");
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  /// Queue item type for source-stage handoff.
  using MessageQueue = folly::coro::BoundedQueue<std::optional<MessageBatch>>;

  /// Queue item type for build-stage handoff.
  using TableSliceQueue
    = folly::coro::BoundedQueue<std::optional<TableSliceFrame>>;

  /// Bundles one configured consumer with callback-lifetime ownership.
  struct SourceConsumer {
    consumer_configuration consumer_cfg;
    Box<RdKafka::KafkaConsumer> consumer;
  };

  /// Owns one partition source (consumer plus async queue wrapper).
  struct PartitionSource {
    int32_t partition = -1;
    std::optional<SourceConsumer> source_consumer;
    std::optional<Box<AsyncConsumerQueue>> queue;
  };

  /// Owns mutable runtime state for source/build/emit stages.
  struct RuntimeState {
    std::vector<PartitionSource> partition_sources;
    std::unordered_map<int32_t, size_t> partition_source_by_id;
    std::shared_ptr<MessageQueue> message_queue;
    std::shared_ptr<TableSliceQueue> table_slice_queue;
    std::map<uint64_t, TableSliceFrame> ordered_slices;
    bool builders_finished = false;
    std::atomic<bool> message_queue_closed = false;
    std::atomic<size_t> live_fetchers = 0;
    std::atomic<size_t> live_builders = 0;
    std::atomic<bool> pipeline_stop_requested = false;
    std::mutex prefetch_budget_mutex;
    size_t in_flight_fetch_bytes = 0;
    Notify prefetch_budget_notify;
    std::atomic<uint64_t> next_fetch_seq = 0;
    uint64_t next_emit_seq = 0;
    std::atomic<uint64_t> scheduled_messages = 0;
  };

  /// Tracks adaptive polling state while collecting one source batch.
  struct FetchPollState {
    duration base_poll_wait = default_fetch_wait_timeout;
    duration poll_wait = default_fetch_wait_timeout;
    size_t consecutive_empty_timeouts = 0;
    std::optional<std::chrono::steady_clock::time_point> batch_deadline;
  };

  /// Starts opt-in perf timing at operator startup.
  auto initialize_perf_tracking() -> void {
    if (not perf_enabled_) {
      return;
    }
    perf_started_ = true;
    perf_start_ = std::chrono::steady_clock::now();
    perf_reported_.store(false, std::memory_order_relaxed);
  }

  /// Parses and validates the configured consumer start offset.
  auto resolve_start_offset(OpCtx& ctx) const -> std::optional<int64_t> {
    auto offset = int64_t{RdKafka::Topic::OFFSET_STORED};
    if (args_.offset and not parse_offset_value(*args_.offset, offset)) {
      diagnostic::error("invalid `offset` value")
        .primary(args_.offset->source)
        .note("must be `beginning`, `end`, `stored`, `<offset>`, or "
              "`-<offset>`")
        .emit(ctx);
      return std::nullopt;
    }
    return offset;
  }

  /// Creates one Kafka consumer with resolved options and callback ownership.
  auto make_source_consumer(OpCtx& ctx, record const& config,
                            ResolvedAwsIamAuth const& auth, int64_t offset)
    -> Task<std::optional<SourceConsumer>> {
    auto cfg = make_consumer_configuration(config, auth.options,
                                           auth.credentials, offset, ctx.dh());
    if (not cfg) {
      diagnostic::error("failed to create kafka configuration: {}", cfg.error())
        .emit(ctx);
      co_return std::nullopt;
    }
    auto source_cfg = std::move(*cfg);
    auto user_options = args_.options;
    if (auth.options) {
      user_options.inner["sasl.mechanism"] = "OAUTHBEARER";
    }
    if (auto ok
        = co_await ctx.resolve_secrets(configure_consumer_or_request_secrets(
          source_cfg, user_options, ctx.dh()));
        not ok) {
      co_return std::nullopt;
    }
    TENZIR_ASSERT(source_cfg.conf);
    auto error = std::string{};
    auto* raw_consumer
      = RdKafka::KafkaConsumer::create(source_cfg.conf.get(), error);
    if (raw_consumer == nullptr) {
      diagnostic::error("failed to create kafka consumer: {}", error).emit(ctx);
      co_return std::nullopt;
    }
    auto source_consumer = SourceConsumer{
      .consumer_cfg = std::move(source_cfg),
      .consumer = Box<RdKafka::KafkaConsumer>::from_unique_ptr(
        std::unique_ptr<RdKafka::KafkaConsumer>{raw_consumer}),
    };
    co_return source_consumer;
  }

  /// Discovers all partition ids for `args_.topic` from broker metadata.
  auto
  discover_topic_partitions(SourceConsumer& source,
                            ResolvedAwsIamAuth const& auth, OpCtx& ctx) const
    -> std::optional<std::vector<int32_t>> {
    auto& consumer = *source.consumer;
    auto conf_value = [&](std::string_view key) -> std::string {
      if (not source.consumer_cfg.conf) {
        return "<unknown>";
      }
      auto value = std::string{};
      if (source.consumer_cfg.conf->get(std::string{key}, value)
            != RdKafka::Conf::ConfResult::CONF_OK
          or value.empty()) {
        return "<unset>";
      }
      return value;
    };
    auto auth_mode =
      [&](std::optional<resolved_aws_credentials> const& creds) -> const char* {
      if (not creds) {
        return "none";
      }
      auto const has_explicit_creds = not creds->access_key_id.empty();
      auto const has_profile = not creds->profile.empty();
      auto const has_role = not creds->role.empty();
      if (has_explicit_creds and has_role) {
        return "explicit+assume_role";
      }
      if (has_profile and has_role) {
        return "profile+assume_role";
      }
      if (has_role) {
        return "default+assume_role";
      }
      if (has_explicit_creds) {
        return "explicit";
      }
      if (has_profile) {
        return "profile";
      }
      return "default_chain";
    };
    auto* metadata = static_cast<RdKafka::Metadata*>(nullptr);
    if (auto err = consumer.metadata(false, nullptr, &metadata, 5000);
        err != RdKafka::ERR_NO_ERROR) {
      auto diag
        = diagnostic::error("failed to query topic metadata for `{}`: {}",
                            args_.topic, RdKafka::err2str(err))
            .note("bootstrap.servers={}", conf_value("bootstrap.servers"))
            .note("security.protocol={}", conf_value("security.protocol"))
            .note("sasl.mechanism={}", conf_value("sasl.mechanism"))
            .note("client.id={}", conf_value("client.id"));
      if (auth.options and auth.credentials) {
        auto const region = auth.credentials->region.empty()
                              ? "<unset>"
                              : auth.credentials->region;
        std::move(diag)
          .note("aws_iam mode={}, region={}", auth_mode(auth.credentials),
                region)
          .hint("this usually indicates broker reachability, TLS, or SASL auth "
                "failure before metadata fetch")
          .emit(ctx);
      } else {
        std::move(diag)
          .hint("check broker reachability, TLS, or SASL auth")
          .emit(ctx);
      }
      return std::nullopt;
    }
    auto metadata_guard = std::unique_ptr<RdKafka::Metadata>{metadata};
    TENZIR_ASSERT(metadata_guard);
    auto partitions = std::vector<int32_t>{};
    for (auto const& topic_meta : *metadata_guard->topics()) {
      if (topic_meta == nullptr or topic_meta->topic() != args_.topic) {
        continue;
      }
      if (topic_meta->err() != RdKafka::ERR_NO_ERROR) {
        diagnostic::error("failed to get topic metadata for `{}`: {}",
                          args_.topic, RdKafka::err2str(topic_meta->err()))
          .emit(ctx);
        return std::nullopt;
      }
      partitions.reserve(topic_meta->partitions()->size());
      for (auto const& partition_meta : *topic_meta->partitions()) {
        if (partition_meta == nullptr) {
          continue;
        }
        if (partition_meta->err() != RdKafka::ERR_NO_ERROR) {
          diagnostic::warning("ignoring partition {} due to metadata error: {}",
                              partition_meta->id(),
                              RdKafka::err2str(partition_meta->err()))
            .emit(ctx);
          continue;
        }
        partitions.push_back(partition_meta->id());
      }
      break;
    }
    if (partitions.empty()) {
      diagnostic::error("topic `{}` has no discoverable partitions",
                        args_.topic)
        .emit(ctx);
      return std::nullopt;
    }
    std::ranges::sort(partitions);
    return partitions;
  }

  /// Builds one dedicated source consumer+queue pair for each topic partition.
  auto make_partition_sources(OpCtx& ctx, ResolvedAwsIamAuth const& auth,
                              int64_t offset) -> Task<bool> {
    auto config = source_global_defaults();
    if (not config.contains("group.id")) {
      config["group.id"] = "tenzir";
    }
    apply_from_kafka_throughput_defaults(config);
    config["enable.auto.commit"] = "false";
    if (args_.exit) {
      config["enable.partition.eof"] = "true";
    }
    runtime_.partition_sources.clear();
    runtime_.partition_source_by_id.clear();
    assigned_partitions_.clear();
    eof_partitions_.clear();
    auto bootstrap_consumer
      = co_await make_source_consumer(ctx, config, auth, offset);
    if (not bootstrap_consumer) {
      co_return false;
    }
    auto partitions = discover_topic_partitions(*bootstrap_consumer, auth, ctx);
    if (not partitions) {
      co_return false;
    }
    runtime_.partition_sources.reserve(partitions->size());
    auto* evb = folly::getGlobalIOExecutor()->getEventBase();
    for (auto i = size_t{0}; i < partitions->size(); ++i) {
      auto partition = (*partitions)[i];
      auto next_consumer = std::optional<SourceConsumer>{};
      if (i == 0) {
        next_consumer = std::move(*bootstrap_consumer);
      } else {
        next_consumer
          = co_await make_source_consumer(ctx, config, auth, offset);
      }
      if (not next_consumer) {
        co_return false;
      }
      auto assignment = std::vector<RdKafka::TopicPartition*>{
        RdKafka::TopicPartition::create(args_.topic, partition, offset),
      };
      auto assign_err = next_consumer->consumer->assign(assignment);
      RdKafka::TopicPartition::destroy(assignment);
      if (assign_err != RdKafka::ERR_NO_ERROR) {
        diagnostic::error("failed to assign partition {}: {}", partition,
                          RdKafka::err2str(assign_err))
          .emit(ctx);
        co_return false;
      }
      auto queue = AsyncConsumerQueue::make(*evb, *next_consumer->consumer);
      if (queue.is_err()) {
        diagnostic::error("failed to create async consumer queue for partition "
                          "{}: {}",
                          partition, std::move(queue).unwrap_err())
          .emit(ctx);
        co_return false;
      }
      auto source = PartitionSource{};
      source.partition = partition;
      source.source_consumer = std::move(*next_consumer);
      source.queue.emplace(std::move(queue).unwrap());
      assigned_partitions_.insert(partition);
      runtime_.partition_source_by_id.emplace(
        partition, runtime_.partition_sources.size());
      runtime_.partition_sources.emplace_back(std::move(source));
    }
    if (runtime_.partition_sources.empty()) {
      diagnostic::error("topic `{}` has no assignable partitions", args_.topic)
        .emit(ctx);
      co_return false;
    }
    co_return true;
  }

  /// Initializes per-run runtime queues, counters, and stage parameters.
  auto initialize_runtime_state() -> void {
    auto parsed_optimization
      = from_string<OptimizationMode>(args_._optimization);
    TENZIR_ASSERT(parsed_optimization);
    optimization_mode_ = *parsed_optimization;
    worker_batch_size_ = resolve_worker_batch_size();
    worker_count_ = resolve_worker_concurrency();
    auto fetch_capacity = static_cast<uint32_t>(std::min<uint64_t>(
      args_._prefetch_batches, std::numeric_limits<uint32_t>::max()));
    TENZIR_ASSERT(fetch_capacity > 0);
    runtime_.message_queue = std::make_shared<MessageQueue>(fetch_capacity);
    runtime_.table_slice_queue
      = std::make_shared<TableSliceQueue>(fetch_capacity);
    runtime_.ordered_slices.clear();
    runtime_.builders_finished = false;
    runtime_.next_emit_seq = 0;
    runtime_.next_fetch_seq.store(0);
    runtime_.scheduled_messages.store(emitted_messages_);
    runtime_.message_queue_closed.store(false);
    runtime_.live_fetchers.store(runtime_.partition_sources.size());
    runtime_.live_builders.store(worker_count_);
    runtime_.pipeline_stop_requested.store(false);
    {
      auto guard = std::scoped_lock{runtime_.prefetch_budget_mutex};
      runtime_.in_flight_fetch_bytes = 0;
    }
  }

  /// Adds `delta` to one instrumentation counter when perf stats are enabled.
  auto add_perf_counter(std::atomic<uint64_t>& counter, uint64_t delta
                                                        = 1) const -> void {
    if (not perf_enabled_) {
      return;
    }
    counter.fetch_add(delta, std::memory_order_relaxed);
  }

  /// Emits a one-time, compact stage timing summary for benchmarking runs.
  auto emit_perf_summary(char const* reason) const -> void {
    if (not perf_enabled_ or not perf_started_) {
      return;
    }
    auto expected = false;
    if (not perf_reported_.compare_exchange_strong(expected, true,
                                                   std::memory_order_relaxed)) {
      return;
    }
    auto load = [](std::atomic<uint64_t> const& counter) {
      return counter.load(std::memory_order_relaxed);
    };
    auto elapsed_ns = std::max<uint64_t>(
      1, as_ns(std::chrono::steady_clock::now() - perf_start_));
    auto elapsed_s = static_cast<double>(elapsed_ns) / 1'000'000'000.0;
    auto emitted_messages = load(perf_.emitted_messages);
    auto eps = static_cast<double>(emitted_messages) / elapsed_s;
    auto queue_perf = AsyncConsumerQueue::ConsumePerfSnapshot{};
    for (auto const& source : runtime_.partition_sources) {
      if (not source.queue) {
        continue;
      }
      auto snapshot = (*source.queue)->consume_perf_snapshot();
      queue_perf.consume_batch_calls += snapshot.consume_batch_calls;
      queue_perf.consume_batch_wait_ns += snapshot.consume_batch_wait_ns;
      queue_perf.consume_batch_wrap_ns += snapshot.consume_batch_wrap_ns;
      queue_perf.consume_batch_timeouts += snapshot.consume_batch_timeouts;
      queue_perf.consume_batch_messages += snapshot.consume_batch_messages;
    }
    TENZIR_WARN(
      "from_kafka perf: reason={} topic={} elapsed_ms={} eps={:.0f} "
      "emitted_messages={} emitted_batches={} emitted_slices={} "
      "fetch_calls={} fetch_wait_ms={} fetch_timeouts={} fetched_batches={} "
      "fetched_messages={} fetched_mb={:.2f} prefetch_wait_ms={} "
      "fetched_enqueue_wait_ms={} build_dequeue_wait_ms_total={} "
      "build_compute_ms_total={} build_enqueue_wait_ms_total={} "
      "built_batches={} built_messages={} "
      "runner_dequeue_wait_ms={} push_wait_ms={} eof_events={} fatal_errors={} "
      "queue_consume_calls={} queue_consume_wait_ms={} "
      "queue_consume_wrap_ms={} queue_consume_timeouts={} "
      "queue_consume_messages={}",
      reason, args_.topic, elapsed_ns / 1'000'000, eps, emitted_messages,
      load(perf_.emitted_batches), load(perf_.emitted_slices),
      load(perf_.fetch_next_batch_calls),
      load(perf_.fetch_next_batch_wait_ns) / 1'000'000,
      load(perf_.fetch_timeouts), load(perf_.fetched_batches),
      load(perf_.fetched_messages),
      static_cast<double>(load(perf_.fetched_payload_bytes))
        / (1024.0 * 1024.0),
      load(perf_.prefetch_wait_ns) / 1'000'000,
      load(perf_.fetched_enqueue_wait_ns) / 1'000'000,
      load(perf_.build_dequeue_wait_ns) / 1'000'000,
      load(perf_.build_compute_ns) / 1'000'000,
      load(perf_.build_enqueue_wait_ns) / 1'000'000, load(perf_.built_batches),
      load(perf_.built_messages),
      load(perf_.runner_dequeue_wait_ns) / 1'000'000,
      load(perf_.push_wait_ns) / 1'000'000, load(perf_.eof_events),
      load(perf_.fatal_errors), queue_perf.consume_batch_calls,
      queue_perf.consume_batch_wait_ns / 1'000'000,
      queue_perf.consume_batch_wrap_ns / 1'000'000,
      queue_perf.consume_batch_timeouts, queue_perf.consume_batch_messages);
  }

  /// Computes the configured worker-side batch size.
  auto resolve_worker_batch_size() const -> size_t {
    auto batch_size = args_._worker_batch_size;
    if (batch_size == 0) {
      batch_size = args_.batch_size;
    }
    if (batch_size == 0) {
      batch_size = 1;
    }
    return static_cast<size_t>(batch_size);
  }

  /// Computes the number of builder workers to spawn.
  auto resolve_worker_concurrency() const -> size_t {
    auto concurrency = args_._worker_concurrency;
    if (concurrency == 0) {
      concurrency = default_worker_concurrency();
    }
    if (concurrency == 0) {
      concurrency = 1;
    }
    return static_cast<size_t>(concurrency);
  }

  /// Returns the next source-batch cap, honoring any `count=` limit.
  auto fetch_batch_size_limit() const -> size_t {
    auto limit = static_cast<uint64_t>(worker_batch_size_);
    if (args_.count) {
      auto scheduled
        = runtime_.scheduled_messages.load(std::memory_order_relaxed);
      if (scheduled >= args_.count->inner) {
        return 0;
      }
      auto remaining = args_.count->inner - scheduled;
      limit = std::min(limit, remaining);
    }
    if (limit == 0) {
      limit = 1;
    }
    return static_cast<size_t>(limit);
  }

  /// Attempts to reserve one output-message slot under `count=` backpressure.
  auto try_reserve_message_slot() const -> bool {
    if (not args_.count) {
      runtime_.scheduled_messages.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    auto limit = args_.count->inner;
    auto scheduled
      = runtime_.scheduled_messages.load(std::memory_order_relaxed);
    while (scheduled < limit) {
      if (runtime_.scheduled_messages.compare_exchange_weak(
            scheduled, scheduled + 1, std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  /// Requests all pipeline stages to stop and wakes budget waiters.
  auto request_pipeline_stop() const -> void {
    if (runtime_.pipeline_stop_requested.exchange(true)) {
      return;
    }
    for (auto& source : runtime_.partition_sources) {
      if (source.queue) {
        (*source.queue)->request_stop();
      }
    }
    runtime_.prefetch_budget_notify.notify_one();
  }

  /// Returns whether any pipeline stage requested shutdown.
  auto is_pipeline_stopping() const -> bool {
    return runtime_.pipeline_stop_requested.load();
  }

  /// Waits until enough byte budget is available for one queued source batch.
  auto acquire_prefetch_budget(size_t bytes) const -> Task<void> {
    if (bytes == 0) {
      co_return;
    }
    while (not is_pipeline_stopping()) {
      {
        auto guard = std::scoped_lock{runtime_.prefetch_budget_mutex};
        // Never block forever on one oversized source batch. Allow exactly one
        // oversized in-flight batch at a time when no other batch is tracked.
        if (bytes > args_._prefetch_bytes) {
          if (runtime_.in_flight_fetch_bytes == 0) {
            runtime_.in_flight_fetch_bytes = bytes;
            co_return;
          }
        } else if (runtime_.in_flight_fetch_bytes + bytes
                   <= args_._prefetch_bytes) {
          runtime_.in_flight_fetch_bytes += bytes;
          co_return;
        }
      }
      co_await runtime_.prefetch_budget_notify.wait();
    }
  }

  /// Releases byte budget after worker-side processing completes.
  auto release_prefetch_budget(size_t bytes) const noexcept -> void {
    if (bytes == 0) {
      return;
    }
    {
      auto guard = std::scoped_lock{runtime_.prefetch_budget_mutex};
      if (bytes >= runtime_.in_flight_fetch_bytes) {
        runtime_.in_flight_fetch_bytes = 0;
      } else {
        runtime_.in_flight_fetch_bytes -= bytes;
      }
    }
    runtime_.prefetch_budget_notify.notify_one();
  }

  /// Enqueues exactly one shutdown sentinel per builder worker.
  auto close_message_queue() const -> Task<void> {
    if (not runtime_.message_queue) {
      co_return;
    }
    auto expected = false;
    if (not runtime_.message_queue_closed.compare_exchange_strong(expected,
                                                                  true)) {
      co_return;
    }
    for (size_t i = 0; i < worker_count_; ++i) {
      co_await runtime_.message_queue->enqueue(std::nullopt);
    }
  }

  /// Converts polled Kafka messages into one source payload batch.
  auto to_fetched_batch(
    std::vector<AsyncConsumerQueue::Message> fetched_messages) const
    -> std::optional<MessageBatch> {
    auto batch = MessageBatch{};
    batch.messages.reserve(fetched_messages.size());
    auto reached_count = false;
    for (auto& message : fetched_messages) {
      switch (message.err()) {
        case RD_KAFKA_RESP_ERR_NO_ERROR: {
          if (not try_reserve_message_slot()) {
            reached_count = true;
            break;
          }
          batch.payload_bytes += message.len();
          batch.messages.push_back(std::move(message));
          break;
        }
        case RD_KAFKA_RESP_ERR__PARTITION_EOF: {
          if (args_.exit) {
            batch.eof_partitions.push_back(message.partition());
          }
          break;
        }
        default: {
          batch.fatal_error
            = fmt::format("unexpected kafka error: `{}`", message.errstr());
          request_pipeline_stop();
          break;
        }
      }
      if (batch.fatal_error) {
        break;
      }
      if (reached_count) {
        break;
      }
    }
    batch.reached_count = reached_count;
    if (batch.messages.empty() and batch.eof_partitions.empty()
        and not batch.fatal_error and not batch.reached_count) {
      return std::nullopt;
    }
    batch.seq = runtime_.next_fetch_seq.fetch_add(1, std::memory_order_relaxed);
    return batch;
  }

  /// Collects one source poll window with adaptive timeout/backoff behavior.
  auto
  collect_fetch_window(AsyncConsumerQueue& queue, size_t max_messages) const
    -> Task<std::vector<AsyncConsumerQueue::Message>> {
    auto pending_messages = std::vector<AsyncConsumerQueue::Message>{};
    pending_messages.reserve(max_messages);
    auto min_wait = std::chrono::duration_cast<duration>(1ms);
    auto state = FetchPollState{};
    state.base_poll_wait = std::max(args_._fetch_wait_timeout, min_wait);
    state.poll_wait = state.base_poll_wait;
    // Allow backoff to grow up to the batch flush horizon, so partial batches
    // do not spin in short timeout polls while waiting for additional records.
    auto poll_wait_cap = std::max(
      state.base_poll_wait,
      std::max(args_._batch_timeout,
               std::chrono::duration_cast<duration>(fetch_wait_backoff_floor)));
    while (pending_messages.size() < max_messages
           and not is_pipeline_stopping()) {
      auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(
        state.poll_wait);
      if (not pending_messages.empty()) {
        TENZIR_ASSERT(state.batch_deadline);
        auto now = std::chrono::steady_clock::now();
        if (now >= *state.batch_deadline) {
          break;
        }
        auto remaining = *state.batch_deadline - now;
        wait = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::min<std::chrono::steady_clock::duration>(remaining,
                                                        state.poll_wait));
        if (wait <= 0ms) {
          wait = 1ms;
        }
      }
      auto next_batch_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        add_perf_counter(perf_.fetch_next_batch_calls);
        next_batch_started = std::chrono::steady_clock::now();
      }
      auto batch = co_await queue.next_batch(
        max_messages - pending_messages.size(), wait);
      if (perf_enabled_) {
        add_perf_counter(
          perf_.fetch_next_batch_wait_ns,
          as_ns(std::chrono::steady_clock::now() - next_batch_started));
      }
      if (batch.messages.empty()) {
        if (batch.timed_out) {
          add_perf_counter(perf_.fetch_timeouts);
          ++state.consecutive_empty_timeouts;
          // Once we already have some data in the current source window, back
          // off on every timeout and keep waiting until the flush deadline.
          auto should_backoff
            = not pending_messages.empty()
              or state.consecutive_empty_timeouts >= fetch_wait_backoff_after;
          if (should_backoff) {
            state.poll_wait = std::min(poll_wait_cap, state.poll_wait * 2);
          }
          if (not pending_messages.empty()) {
            TENZIR_ASSERT(state.batch_deadline);
            if (std::chrono::steady_clock::now() < *state.batch_deadline) {
              continue;
            }
            break;
          }
          continue;
        }
        break;
      }
      state.consecutive_empty_timeouts = 0;
      state.poll_wait = state.base_poll_wait;
      pending_messages.append_range(batch.messages | std::views::as_rvalue);
      if (not state.batch_deadline) {
        state.batch_deadline = std::chrono::steady_clock::now()
                               + std::max(args_._batch_timeout, min_wait);
      }
    }
    co_return pending_messages;
  }

  /// Enqueues one source batch while honoring prefetch-byte budget.
  auto enqueue_fetched_batch(MessageBatch fetched) const -> Task<bool> {
    if (perf_enabled_) {
      add_perf_counter(perf_.fetched_batches);
      add_perf_counter(perf_.fetched_messages,
                       static_cast<uint64_t>(fetched.messages.size()));
      add_perf_counter(perf_.fetched_payload_bytes, fetched.payload_bytes);
    }
    auto reserved_bytes = fetched.payload_bytes;
    if (reserved_bytes > 0) {
      auto budget_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        budget_started = std::chrono::steady_clock::now();
      }
      co_await acquire_prefetch_budget(reserved_bytes);
      if (perf_enabled_) {
        add_perf_counter(
          perf_.prefetch_wait_ns,
          as_ns(std::chrono::steady_clock::now() - budget_started));
      }
      if (is_pipeline_stopping()) {
        release_prefetch_budget(reserved_bytes);
        co_return false;
      }
    }
    auto release_budget = tenzir::detail::scope_guard{
      [this, reserved_bytes]() noexcept {
        release_prefetch_budget(reserved_bytes);
      },
    };
    auto has_fatal = fetched.fatal_error.has_value();
    auto reached_count = fetched.reached_count;
    auto enqueue_started = std::chrono::steady_clock::time_point{};
    if (perf_enabled_) {
      enqueue_started = std::chrono::steady_clock::now();
    }
    co_await runtime_.message_queue->enqueue(
      std::optional<MessageBatch>{std::move(fetched)});
    if (perf_enabled_) {
      add_perf_counter(
        perf_.fetched_enqueue_wait_ns,
        as_ns(std::chrono::steady_clock::now() - enqueue_started));
    }
    release_budget.disable();
    if (has_fatal or reached_count) {
      if (has_fatal) {
        add_perf_counter(perf_.fatal_errors);
      }
      request_pipeline_stop();
      co_return false;
    }
    co_return not is_pipeline_stopping();
  }

  /// Polls one partition source and hands message batches to build workers.
  auto fetch_loop(size_t source_index) const -> Task<void> {
    auto finish_fetcher = [this]() -> Task<void> {
      if (runtime_.live_fetchers.fetch_sub(1) == 1) {
        co_await close_message_queue();
      }
      co_return;
    };
    if (not runtime_.message_queue
        or source_index >= runtime_.partition_sources.size()) {
      co_await finish_fetcher();
      co_return;
    }
    auto& source = runtime_.partition_sources[source_index];
    if (not source.queue) {
      co_await finish_fetcher();
      co_return;
    }
    try {
      while (not is_pipeline_stopping()) {
        auto max_messages = fetch_batch_size_limit();
        if (max_messages == 0) {
          request_pipeline_stop();
          break;
        }
        auto pending_messages
          = co_await collect_fetch_window(**source.queue, max_messages);
        if (pending_messages.empty()) {
          if (is_pipeline_stopping()) {
            break;
          }
          continue;
        }
        auto fetched = to_fetched_batch(std::move(pending_messages));
        if (not fetched) {
          continue;
        }
        if (not co_await enqueue_fetched_batch(std::move(*fetched))) {
          break;
        }
      }
    } catch (folly::OperationCancelled const&) {
      request_pipeline_stop();
    }
    co_await finish_fetcher();
  }

  /// Runs one CPU-stage worker that builds slices from source batches.
  auto build_loop() const -> Task<void> {
    if (not runtime_.message_queue or not runtime_.table_slice_queue) {
      co_return;
    }
    auto diag_handler = BufferingDiagnosticHandler{};
    auto builder_ptr = static_cast<multi_series_builder*>(nullptr);
    auto standalone_builder = std::optional<multi_series_builder>{};
    auto builder_opts = multi_series_builder::options{};
    builder_opts.settings.ordered
      = optimization_mode_ == OptimizationMode::ordered;
    builder_opts.settings.raw = args_._raw;
    auto parser = std::optional<json::ndjson_parser>{};
    if (args_._json) {
      parser.emplace("json", diag_handler, builder_opts);
      builder_ptr = &parser->builder;
    } else {
      auto opts = builder_opts;
      opts.settings.default_schema_name = "tenzir.kafka";
      standalone_builder.emplace(std::move(opts), diag_handler);
      builder_ptr = &*standalone_builder;
    }
    try {
      while (true) {
        auto dequeue_started = std::chrono::steady_clock::time_point{};
        if (perf_enabled_) {
          dequeue_started = std::chrono::steady_clock::now();
        }
        auto next = co_await runtime_.message_queue->dequeue();
        if (perf_enabled_) {
          add_perf_counter(
            perf_.build_dequeue_wait_ns,
            as_ns(std::chrono::steady_clock::now() - dequeue_started));
        }
        if (not next) {
          break;
        }
        auto fetched = std::move(*next);
        auto release_budget = tenzir::detail::scope_guard{
          [this, bytes = fetched.payload_bytes]() noexcept {
            release_prefetch_budget(bytes);
          },
        };
        auto build_started = std::chrono::steady_clock::time_point{};
        if (perf_enabled_) {
          build_started = std::chrono::steady_clock::now();
        }
        auto flush = optimization_mode_ == OptimizationMode::ordered;
        auto frame = args_._json
                       ? build_table_slice(
                           fetched, parser->builder, flush,
                           [&](std::span<const std::byte> payload) {
                             auto const* chars
                               = reinterpret_cast<char const*>(payload.data());
                             auto padded
                               = simdjson::padded_string{chars, payload.size()};
                             parser->parse(padded);
                           })
                       : build_table_slice(
                           fetched, *standalone_builder, flush,
                           [&](std::span<const std::byte> payload) {
                             auto const* chars
                               = reinterpret_cast<char const*>(payload.data());
                             auto sv = std::string_view{chars, payload.size()};
                             standalone_builder->record().field("message").data(
                               std::string{sv});
                           });
        if (perf_enabled_) {
          add_perf_counter(
            perf_.build_compute_ns,
            as_ns(std::chrono::steady_clock::now() - build_started));
          add_perf_counter(perf_.built_batches);
          add_perf_counter(perf_.built_messages,
                           static_cast<uint64_t>(frame.message_count));
        }
        // Attach buffered diagnostics to the frame.
        if (not diag_handler.empty()) {
          frame.diagnostics = diag_handler.drain();
        }
        release_budget.trigger();
        auto enqueue_started = std::chrono::steady_clock::time_point{};
        if (perf_enabled_) {
          enqueue_started = std::chrono::steady_clock::now();
        }
        co_await runtime_.table_slice_queue->enqueue(std::move(frame));
        if (perf_enabled_) {
          add_perf_counter(
            perf_.build_enqueue_wait_ns,
            as_ns(std::chrono::steady_clock::now() - enqueue_started));
        }
      }
      // Flush remaining data from the builder.
      auto final_slices = builder_ptr->finalize_as_table_slice();
      if (not final_slices.empty()) {
        auto frame = TableSliceFrame{};
        frame.seq
          = runtime_.next_fetch_seq.fetch_add(1, std::memory_order_relaxed);
        frame.slices = std::move(final_slices);
        co_await runtime_.table_slice_queue->enqueue(std::move(frame));
      }
    } catch (folly::OperationCancelled const&) {
      request_pipeline_stop();
    }
    if (runtime_.live_builders.fetch_sub(1) == 1
        and runtime_.table_slice_queue) {
      co_await runtime_.table_slice_queue->enqueue(std::nullopt);
    }
  }

  /// Waits for the next in-order table-slice frame from worker output.
  auto await_ordered_batch() const -> Task<TableSliceResult> {
    TENZIR_ASSERT(runtime_.table_slice_queue);
    while (true) {
      auto ready = runtime_.ordered_slices.find(runtime_.next_emit_seq);
      // When builders are finished, some sequence numbers may have been
      // dropped (e.g., a fetched batch was discarded during shutdown before
      // reaching the build stage). Skip gaps and drain in map order.
      if (ready == runtime_.ordered_slices.end()
          and runtime_.builders_finished) {
        if (runtime_.ordered_slices.empty()) {
          co_return TableSliceResult{.end_of_stream = true};
        }
        ready = runtime_.ordered_slices.begin();
        runtime_.next_emit_seq = ready->first;
      }
      if (ready != runtime_.ordered_slices.end()) {
        auto batch = std::move(ready->second);
        runtime_.ordered_slices.erase(ready);
        ++runtime_.next_emit_seq;
        co_return TableSliceResult{
          .frame = std::move(batch),
        };
      }
      auto dequeue_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        dequeue_started = std::chrono::steady_clock::now();
      }
      auto next = co_await runtime_.table_slice_queue->dequeue();
      if (perf_enabled_) {
        add_perf_counter(
          perf_.runner_dequeue_wait_ns,
          as_ns(std::chrono::steady_clock::now() - dequeue_started));
      }
      if (not next) {
        runtime_.builders_finished = true;
        continue;
      }
      if (next->seq == runtime_.next_emit_seq) {
        ++runtime_.next_emit_seq;
        co_return TableSliceResult{
          .frame = std::move(*next),
        };
      }
      auto [_, inserted]
        = runtime_.ordered_slices.emplace(next->seq, std::move(*next));
      TENZIR_ASSERT(inserted);
    }
  }

  /// Moves per-partition offsets from one emitted batch into checkpoint state.
  auto advance_checkpoint(std::unordered_map<int32_t, int64_t> const& offsets)
    -> void {
    for (auto const& [partition, offset] : offsets) {
      checkpoint_pending_offsets_[partition] = offset + 1;
    }
  }

  /// Tracks partition EOF notifications and marks the source done if complete.
  auto mark_partition_eof(int32_t partition) -> void {
    if (not args_.exit or not assigned_partitions_.contains(partition)) {
      return;
    }
    eof_partitions_.insert(partition);
    if (eof_partitions_.size() == assigned_partitions_.size()) {
      // Don't set done_ here; let the pipeline drain so the build_loop
      // can finalize and flush any buffered data from the builder.
      request_pipeline_stop();
    }
  }

  FromKafkaArgs args_;
  OptimizationMode optimization_mode_ = OptimizationMode::ordered;
  size_t worker_count_ = 1;
  size_t worker_batch_size_ = 1;
  mutable RuntimeState runtime_;
  // Number of records emitted so far; used for `count=` resumption.
  size_t emitted_messages_ = 0;
  // Invariant: committed offsets are stored as "next offset to consume".
  std::unordered_map<int32_t, int64_t> checkpoint_pending_offsets_;
  // Invariant: fixed partition set assigned at startup.
  std::unordered_set<int32_t> assigned_partitions_;
  // Invariant: contains only partitions from `assigned_partitions_`.
  std::unordered_set<int32_t> eof_partitions_;
  // Optional counters for benchmarking; enabled via env flag.
  mutable FromKafkaPerfCounters perf_;
  mutable std::atomic<bool> perf_reported_ = false;
  bool perf_enabled_ = false;
  bool perf_started_ = false;
  std::chrono::steady_clock::time_point perf_start_{};
  bool done_ = false;
};

/// Plugin entrypoint that parses `from_kafka` arguments and builds operators.
class FromKafkaPlugin final
  : public virtual operator_plugin2<legacy::from_kafka_operator>,
    public virtual OperatorPlugin {
public:
  auto initialize(record const& unused_plugin_config,
                  record const& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `kafka.yaml` instead",
                               this->name())
        .to_error();
    }
    auto defaults = record{};
    [&] {
      auto ptr = global_config.find("plugins");
      if (ptr == global_config.end()) {
        return;
      }
      auto const* plugin_config = try_as<record>(&ptr->second);
      if (not plugin_config) {
        return;
      }
      auto kafka_config_ptr = plugin_config->find("kafka");
      if (kafka_config_ptr == plugin_config->end()) {
        return;
      }
      auto const* kafka_config = try_as<record>(&kafka_config_ptr->second);
      if (not kafka_config or kafka_config->empty()) {
        return;
      }
      defaults = flatten(*kafka_config);
    }();
    if (not defaults.contains("bootstrap.servers")) {
      defaults["bootstrap.servers"] = "localhost";
    }
    if (not defaults.contains("client.id")) {
      defaults["client.id"] = "tenzir";
    }
    source_global_defaults() = std::move(defaults);
    return caf::none;
  }

  auto name() const -> std::string override {
    return "from_kafka";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    return legacy::make_from_kafka(std::move(inv), ctx,
                                   source_global_defaults());
  }

  auto describe() const -> Description override {
    auto initial = FromKafkaArgs{};
    initial.options = located{record{}, location::unknown};
    auto d = Describer<FromKafkaArgs, FromKafka>{std::move(initial)};
    d.positional("topic", &FromKafkaArgs::topic);
    d.named("count", &FromKafkaArgs::count);
    auto exit_arg = d.named("exit", &FromKafkaArgs::exit);
    auto offset_arg = d.named("offset", &FromKafkaArgs::offset);
    auto optimization_arg
      = d.named_optional("_optimization", &FromKafkaArgs::_optimization);
    auto options_arg = d.named_optional("options", &FromKafkaArgs::options);
    auto aws_region_arg = d.named("aws_region", &FromKafkaArgs::aws_region);
    auto aws_iam_arg = d.named("aws_iam", &FromKafkaArgs::aws_iam);
    auto batch_size_arg
      = d.named_optional("batch_size", &FromKafkaArgs::batch_size);
    auto worker_batch_size_arg = d.named_optional(
      "_worker_batch_size", &FromKafkaArgs::_worker_batch_size);
    auto worker_concurrency_arg = d.named_optional(
      "_worker_concurrency", &FromKafkaArgs::_worker_concurrency);
    auto prefetch_batches_arg = d.named_optional(
      "_prefetch_batches", &FromKafkaArgs::_prefetch_batches);
    auto prefetch_bytes_arg
      = d.named_optional("_prefetch_bytes", &FromKafkaArgs::_prefetch_bytes);
    auto batch_timeout_arg
      = d.named_optional("_batch_timeout", &FromKafkaArgs::_batch_timeout);
    auto fetch_wait_timeout_arg = d.named_optional(
      "_fetch_wait_timeout", &FromKafkaArgs::_fetch_wait_timeout);
    d.named_optional("_json", &FromKafkaArgs::_json);
    d.named_optional("_raw", &FromKafkaArgs::_raw);
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto mode = OptimizationMode::ordered;
      if (auto optimization = ctx.get(optimization_arg); optimization) {
        auto parsed = from_string<OptimizationMode>(*optimization);
        if (not parsed) {
          diagnostic::error("invalid `_optimization` value `{}`", *optimization)
            .primary(
              ctx.get_location(optimization_arg).value_or(location::unknown))
            .note("must be either `ordered` or `unordered`")
            .emit(ctx);
          return {};
        }
        mode = *parsed;
      }
      if (auto batch_size = ctx.get(batch_size_arg); batch_size) {
        if (*batch_size == 0) {
          diagnostic::error("`batch_size` must be greater than zero")
            .primary(
              ctx.get_location(batch_size_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto worker_batch_size = ctx.get(worker_batch_size_arg);
          worker_batch_size) {
        if (*worker_batch_size == 0) {
          diagnostic::error("`_worker_batch_size` must be greater than zero")
            .primary(ctx.get_location(worker_batch_size_arg)
                       .value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto worker_concurrency = ctx.get(worker_concurrency_arg);
          worker_concurrency) {
        if (*worker_concurrency == 0) {
          diagnostic::error("`_worker_concurrency` must be greater than zero")
            .primary(ctx.get_location(worker_concurrency_arg)
                       .value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto prefetch_batches = ctx.get(prefetch_batches_arg);
          prefetch_batches) {
        if (*prefetch_batches == 0) {
          diagnostic::error("`_prefetch_batches` must be greater than zero")
            .primary(
              ctx.get_location(prefetch_batches_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto prefetch_bytes = ctx.get(prefetch_bytes_arg); prefetch_bytes) {
        if (*prefetch_bytes == 0) {
          diagnostic::error("`_prefetch_bytes` must be greater than zero")
            .primary(
              ctx.get_location(prefetch_bytes_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto batch_timeout = ctx.get(batch_timeout_arg); batch_timeout) {
        if (*batch_timeout <= duration::zero()) {
          diagnostic::error("`_batch_timeout` must be a positive duration")
            .primary(
              ctx.get_location(batch_timeout_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto fetch_wait_timeout = ctx.get(fetch_wait_timeout_arg);
          fetch_wait_timeout) {
        if (*fetch_wait_timeout <= duration::zero()) {
          diagnostic::error("`_fetch_wait_timeout` must be a positive duration")
            .primary(ctx.get_location(fetch_wait_timeout_arg)
                       .value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto options = ctx.get(options_arg); options) {
        if (not validate_options(*options, ctx)) {
          return {};
        }
        if (options->inner.contains("enable.auto.commit")) {
          diagnostic::error("`enable.auto.commit` must not be specified")
            .primary(options->source)
            .note("`enable.auto.commit` is enforced to be `false`")
            .emit(ctx);
          return {};
        }
      }
      if (auto offset = ctx.get(offset_arg); offset) {
        auto parsed = int64_t{};
        if (not parse_offset_value(*offset, parsed)) {
          diagnostic::error("invalid `offset` value")
            .primary(offset->source)
            .note("must be `beginning`, `end`, `stored`, `<offset>`, or "
                  "`-<offset>`")
            .emit(ctx);
          return {};
        }
      }
      if (auto iam = ctx.get(aws_iam_arg); iam) {
        if (auto options = ctx.get(options_arg); options) {
          if (not check_sasl_mechanism(*options, ctx)) {
            return {};
          }
        }
        auto aws = aws_iam_options::from_record(*iam, ctx);
        if (not aws) {
          return {};
        }
        if (not ctx.get(aws_region_arg) and not aws->region) {
          diagnostic::error(
            "`aws_region` is required for Kafka MSK authentication")
            .primary(iam->source.subloc(0, 1))
            .emit(ctx);
          return {};
        }
      }
      if (mode == OptimizationMode::unordered) {
        if (ctx.get(exit_arg)) {
          diagnostic::error(
            "`_optimization=\"unordered\"` is incompatible with `exit`")
            .primary(
              ctx.get_location(optimization_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::FromKafkaPlugin)
