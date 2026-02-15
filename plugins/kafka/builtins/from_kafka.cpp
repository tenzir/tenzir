//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/async_consumer.hpp"
#include "kafka/configuration.hpp"
#include "kafka/message_builder.hpp"
#include "kafka/operator.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/aws_iam.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/scope_guard.hpp"

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
#include <cctype>
#include <chrono>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tenzir::plugins::kafka {

namespace {

/// Default delay before flushing a partial batch.
constexpr auto default_batch_timeout = 100ms;

/// Default upper bound for one Kafka notification wait while fetching.
constexpr auto default_fetch_wait_timeout = 1ms;

/// Maximum adaptive wait used after repeated empty fetch timeouts.
constexpr auto fetch_wait_backoff_cap = 16ms;

/// Number of consecutive idle timeouts before doubling fetch wait.
constexpr auto fetch_wait_backoff_after = 8uz;

/// Stores process-wide `from_kafka` defaults from `kafka.yaml`.
auto source_global_defaults() -> record& {
  static auto defaults = record{};
  return defaults;
}

/// Returns throughput defaults for librdkafka consumer-side queueing/fetching.
auto from_kafka_throughput_defaults() {
  using entry = std::pair<std::string_view, std::string_view>;
  // Performance note: these defaults intentionally bias towards low broker-side
  // holdback (`fetch.min.bytes=1`, `fetch.wait.max.ms=500`) while increasing
  // client queue depth. Local 10M-message fixture runs showed this combination
  // avoids source stalls better than aggressive "large fetch" defaults.
  return std::to_array<entry>({{"fetch.min.bytes", "1"},
                            {"fetch.wait.max.ms", "500"},
                            {"max.partition.fetch.bytes", "1048576"},
                            {"queued.min.messages", "200000"},
                            {"queued.max.messages.kbytes", "262144"}});
}

/// Adds throughput defaults unless explicitly configured via `kafka.yaml`.
/// User-supplied `from_kafka options={...}` are still applied afterwards.
auto apply_from_kafka_throughput_defaults(record& config) -> void {
  for (const auto& [key, value] : from_kafka_throughput_defaults()) {
    auto key_string = std::string{key};
    if (config.contains(key_string)) {
      continue;
    }
    config[key_string] = std::string{value};
  }
}

/// Parsed arguments for `from_kafka`.
struct FromKafkaArgs {
  std::string topic;
  std::optional<located<uint64_t>> count;
  std::optional<location> exit;
  std::optional<located<data>> offset;
  std::string optimization = "ordered";
  uint64_t batch_size = 10'000;
  uint64_t worker_batch_size = 0;
  uint64_t worker_concurrency = 0;
  uint64_t prefetch_batches = 8;
  uint64_t prefetch_bytes = 256ull * 1024ull * 1024ull;
  duration batch_timeout = default_batch_timeout;
  duration fetch_wait_timeout = default_fetch_wait_timeout;
  located<record> options;
  std::optional<located<std::string>> aws_region;
  std::optional<located<record>> aws_iam;
};

/// Enumerates batching behavior for emitting processed Kafka records.
TENZIR_ENUM(OptimizationMode, ordered, unordered);

/// Parses a boolean-like environment flag (`1`, `true`, `yes`, `on`).
auto parse_bool_flag(std::string value) -> bool {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) -> char {
                   return static_cast<char>(std::tolower(c));
                 });
  return value == "1" or value == "true" or value == "yes" or value == "on";
}

/// Returns whether opt-in `from_kafka` perf counters should be collected.
auto from_kafka_perf_stats_enabled() -> bool {
  static auto enabled = [] {
    auto value = tenzir::detail::getenv("TENZIR_KAFKA_FROM_PERF_STATS");
    if (not value) {
      return false;
    }
    return parse_bool_flag(*value);
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
struct FromKafkaPerfCounters {
  std::atomic<uint64_t> fetch_next_batch_calls = 0;
  std::atomic<uint64_t> fetch_next_batch_wait_ns = 0;
  std::atomic<uint64_t> fetch_timeouts = 0;
  std::atomic<uint64_t> fetched_batches = 0;
  std::atomic<uint64_t> fetched_messages = 0;
  std::atomic<uint64_t> fetched_payload_bytes = 0;
  std::atomic<uint64_t> prefetch_wait_ns = 0;
  std::atomic<uint64_t> fetched_enqueue_wait_ns = 0;
  std::atomic<uint64_t> build_dequeue_wait_ns = 0;
  std::atomic<uint64_t> build_compute_ns = 0;
  std::atomic<uint64_t> build_enqueue_wait_ns = 0;
  std::atomic<uint64_t> built_batches = 0;
  std::atomic<uint64_t> built_messages = 0;
  std::atomic<uint64_t> runner_dequeue_wait_ns = 0;
  std::atomic<uint64_t> push_wait_ns = 0;
  std::atomic<uint64_t> emitted_batches = 0;
  std::atomic<uint64_t> emitted_messages = 0;
  std::atomic<uint64_t> emitted_slices = 0;
  std::atomic<uint64_t> eof_events = 0;
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
auto parse_offset_value(const located<data>& input, int64_t& offset) -> bool {
  return match(
    input.inner,
    [&](const std::string& value) -> bool {
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
    [&](const auto&) -> bool {
      return false;
    });
}

/// Represents one fetched Kafka batch plus control metadata.
struct FetchedBatch {
  uint64_t seq = 0;
  std::vector<AsyncConsumerQueue::Message> messages;
  std::vector<int32_t> eof_partitions;
  std::optional<std::string> fatal_error;
  bool reached_count = false;
  size_t payload_bytes = 0;
};

/// Represents one built table-slice batch plus commit metadata.
struct BuiltBatch {
  uint64_t seq = 0;
  std::optional<table_slice> slice;
  std::unordered_map<int32_t, int64_t> max_offsets;
  size_t message_count = 0;
  std::vector<int32_t> eof_partitions;
  std::optional<std::string> fatal_error;
};

/// Result envelope returned by `await_task()` to `process_task()`.
struct FromKafkaTaskResult {
  std::optional<BuiltBatch> batch;
  bool end_of_stream = false;
};

/// Streaming source operator that consumes Kafka records asynchronously.
class FromKafkaOperator final : public Operator<void, table_slice> {
public:
  explicit FromKafkaOperator(FromKafkaArgs args) : args_{std::move(args)} {
    perf_enabled_ = from_kafka_perf_stats_enabled();
  }
  FromKafkaOperator(FromKafkaOperator&& other) noexcept
    : args_{std::move(other.args_)},
      optimization_mode_{other.optimization_mode_},
      worker_count_{other.worker_count_},
      worker_batch_size_{other.worker_batch_size_},
      cfg_{std::move(other.cfg_)},
      consumer_{std::move(other.consumer_)},
      queue_{std::move(other.queue_)},
      fetched_queue_{std::move(other.fetched_queue_)},
      built_queue_{std::move(other.built_queue_)},
      ordered_batches_{std::move(other.ordered_batches_)},
      in_flight_fetch_bytes_{other.in_flight_fetch_bytes_},
      next_fetch_seq_{other.next_fetch_seq_},
      next_emit_seq_{other.next_emit_seq_},
      scheduled_messages_{other.scheduled_messages_},
      emitted_messages_{other.emitted_messages_},
      checkpoint_pending_offsets_{std::move(other.checkpoint_pending_offsets_)},
      assigned_partitions_{std::move(other.assigned_partitions_)},
      eof_partitions_{std::move(other.eof_partitions_)},
      perf_enabled_{other.perf_enabled_},
      perf_started_{other.perf_started_},
      perf_start_{other.perf_start_},
      done_{other.done_} {
    fetched_queue_closed_.store(other.fetched_queue_closed_.load());
    live_builders_.store(other.live_builders_.load());
    pipeline_stop_requested_.store(other.pipeline_stop_requested_.load());
  }
  auto operator=(FromKafkaOperator&&) -> FromKafkaOperator& = delete;
  FromKafkaOperator(const FromKafkaOperator&) = delete;
  auto operator=(const FromKafkaOperator&) -> FromKafkaOperator& = delete;

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    if (done_) {
      co_return;
    }
    if (perf_enabled_) {
      perf_started_ = true;
      perf_start_ = std::chrono::steady_clock::now();
      perf_reported_.store(false, std::memory_order_relaxed);
    }
    auto aws = std::optional<aws_iam_options>{};
    if (args_.aws_iam) {
      auto parsed = aws_iam_options::from_record(*args_.aws_iam, ctx.dh());
      if (not parsed) {
        done_ = true;
        co_return;
      }
      aws = std::move(*parsed);
      if (not args_.aws_region and not aws->region) {
        diagnostic::error(
          "`aws_region` is required for Kafka MSK authentication")
          .primary(args_.aws_iam->source)
          .emit(ctx);
        done_ = true;
        co_return;
      }
    }
    auto resolved_creds = std::optional<resolved_aws_credentials>{};
    if (aws and (aws->has_explicit_credentials() or aws->role)) {
      resolved_creds.emplace();
      auto requests = aws->make_secret_requests(*resolved_creds, ctx.dh());
      if (auto ok = co_await ctx.resolve_secrets(std::move(requests)); not ok) {
        done_ = true;
        co_return;
      }
    }
    if (args_.aws_region) {
      if (not resolved_creds) {
        resolved_creds.emplace();
      }
      resolved_creds->region = args_.aws_region->inner;
    }
    auto config = source_global_defaults();
    if (not config.contains("group.id")) {
      config["group.id"] = "tenzir";
    }
    apply_from_kafka_throughput_defaults(config);
    auto cfg = configuration::make(config, aws, resolved_creds, ctx.dh());
    if (not cfg) {
      diagnostic::error("failed to create kafka configuration: {}", cfg.error())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    cfg_ = std::move(*cfg);
    auto user_options = args_.options;
    if (aws) {
      user_options.inner["sasl.mechanism"] = "OAUTHBEARER";
    }
    if (auto ok = co_await ctx.resolve_secrets(
          configure_or_request(user_options, *cfg_, ctx.dh()));
        not ok) {
      done_ = true;
      co_return;
    }
    if (args_.exit) {
      if (auto err = cfg_->set("enable.partition.eof", "true"); err) {
        diagnostic::error("failed to enable partition EOF: {}", err).emit(ctx);
        done_ = true;
        co_return;
      }
    }
    if (auto err = cfg_->set("enable.auto.commit", "false"); err) {
      diagnostic::error("failed to disable auto-commit: {}", err).emit(ctx);
      done_ = true;
      co_return;
    }
    auto offset = int64_t{RdKafka::Topic::OFFSET_STORED};
    if (args_.offset and not parse_offset_value(*args_.offset, offset)) {
      diagnostic::error("invalid `offset` value")
        .primary(args_.offset->source)
        .note(
          "must be `beginning`, `end`, `stored`, `<offset>`, or `-<offset>`")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (auto err = cfg_->set_rebalance_cb(offset); err) {
      diagnostic::error("failed to set rebalance callback: {}", err).emit(ctx);
      done_ = true;
      co_return;
    }
    auto error = std::string{};
    auto* raw_consumer
      = RdKafka::KafkaConsumer::create(cfg_->underlying(), error);
    if (raw_consumer == nullptr) {
      diagnostic::error("failed to create kafka consumer: {}", error).emit(ctx);
      done_ = true;
      co_return;
    }
    consumer_.emplace(Box<RdKafka::KafkaConsumer>::from_unique_ptr(
      std::unique_ptr<RdKafka::KafkaConsumer>{raw_consumer}));
    if (auto err = (*consumer_)->subscribe({args_.topic});
        err != RdKafka::ERR_NO_ERROR) {
      diagnostic::error("failed to subscribe to topic: {}",
                        RdKafka::err2str(err))
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto* evb = folly::getGlobalIOExecutor()->getEventBase();
    auto queue = AsyncConsumerQueue::make(*evb, **consumer_);
    if (queue.is_err()) {
      diagnostic::error("failed to create async consumer queue: {}",
                        std::move(queue).unwrap_err())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    queue_.emplace(std::move(queue).unwrap());
    auto parsed_optimization
      = from_string<OptimizationMode>(args_.optimization);
    TENZIR_ASSERT(parsed_optimization);
    optimization_mode_ = *parsed_optimization;
    worker_batch_size_ = resolve_worker_batch_size();
    worker_count_ = resolve_worker_concurrency();
    auto fetch_capacity = static_cast<uint32_t>(std::min<uint64_t>(
      args_.prefetch_batches, std::numeric_limits<uint32_t>::max()));
    TENZIR_ASSERT(fetch_capacity > 0);
    fetched_queue_ = std::make_shared<FetchedQueue>(fetch_capacity);
    built_queue_ = std::make_shared<BuiltQueue>(fetch_capacity);
    ordered_batches_.clear();
    next_emit_seq_ = 0;
    next_fetch_seq_ = 0;
    scheduled_messages_ = emitted_messages_;
    fetched_queue_closed_.store(false);
    live_builders_.store(worker_count_);
    pipeline_stop_requested_.store(false);
    {
      auto guard = std::scoped_lock{prefetch_budget_mutex_};
      in_flight_fetch_bytes_ = 0;
    }
    ctx.spawn_task(
      folly::coro::co_withExecutor(folly::getGlobalIOExecutor(), fetch_loop()));
    for (size_t i = 0; i < worker_count_; ++i) {
      ctx.spawn_task(folly::coro::co_withExecutor(folly::getGlobalCPUExecutor(),
                                                  build_loop()));
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (not built_queue_) {
      co_return FromKafkaTaskResult{.end_of_stream = true};
    }
    try {
      auto token = co_await folly::coro::co_current_cancellation_token;
      if (token.isCancellationRequested()) {
        request_pipeline_stop();
        co_return FromKafkaTaskResult{.end_of_stream = true};
      }
      if (optimization_mode_ == OptimizationMode::ordered) {
        co_return co_await await_ordered_batch();
      }
      auto dequeue_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        dequeue_started = std::chrono::steady_clock::now();
      }
      auto next = co_await built_queue_->dequeue();
      if (perf_enabled_) {
        add_perf_counter(
          perf_.runner_dequeue_wait_ns,
          as_ns(std::chrono::steady_clock::now() - dequeue_started));
      }
      if (not next) {
        emit_perf_summary("end_of_stream");
        co_return FromKafkaTaskResult{.end_of_stream = true};
      }
      co_return FromKafkaTaskResult{
        .batch = std::move(*next),
      };
    } catch (const folly::OperationCancelled&) {
      request_pipeline_stop();
      emit_perf_summary("cancelled");
      co_return FromKafkaTaskResult{.end_of_stream = true};
    }
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto task_result = std::move(result).as<FromKafkaTaskResult>();
    if (task_result.end_of_stream) {
      done_ = true;
      emit_perf_summary("end_of_stream");
      request_pipeline_stop();
      co_return;
    }
    TENZIR_ASSERT(task_result.batch);
    auto batch = std::move(*task_result.batch);
    if (batch.slice) {
      auto push_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        push_started = std::chrono::steady_clock::now();
      }
      co_await push(std::move(*batch.slice));
      if (perf_enabled_) {
        add_perf_counter(
          perf_.push_wait_ns,
          as_ns(std::chrono::steady_clock::now() - push_started));
        add_perf_counter(perf_.emitted_slices);
      }
      advance_checkpoint(batch.max_offsets);
      co_await commit_pending_offsets();
      emitted_messages_ += batch.message_count;
      add_perf_counter(perf_.emitted_batches);
      add_perf_counter(perf_.emitted_messages,
                       static_cast<uint64_t>(batch.message_count));
    }
    for (auto partition : batch.eof_partitions) {
      if (done_) {
        break;
      }
      add_perf_counter(perf_.eof_events);
      mark_partition_eof(partition, ctx);
    }
    if (batch.fatal_error) {
      diagnostic::error("{}", *batch.fatal_error).emit(ctx);
      done_ = true;
      add_perf_counter(perf_.fatal_errors);
    }
    if (args_.count and emitted_messages_ >= args_.count->inner) {
      done_ = true;
    }
    if (done_) {
      emit_perf_summary("done");
      request_pipeline_stop();
    }
  }

  auto post_commit() -> Task<void> override {
    co_await commit_pending_offsets();
  }

  auto commit_pending_offsets() -> Task<void> {
    if (checkpoint_pending_offsets_.empty() or not consumer_) {
      co_return;
    }
    auto offsets = std::vector<RdKafka::TopicPartition*>{};
    offsets.reserve(checkpoint_pending_offsets_.size());
    for (const auto& [partition, offset] : checkpoint_pending_offsets_) {
      offsets.push_back(
        RdKafka::TopicPartition::create(args_.topic, partition, offset));
    }
    if (auto err = (*consumer_)->commitSync(offsets);
        err != RdKafka::ERR_NO_ERROR) {
      TENZIR_WARN("from_kafka: failed to commit offsets: {}",
                  RdKafka::err2str(err));
    }
    RdKafka::TopicPartition::destroy(offsets);
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
  /// Queue item type for fetch-stage handoff.
  using FetchedQueue = folly::coro::BoundedQueue<std::optional<FetchedBatch>>;

  /// Queue item type for build-stage handoff.
  using BuiltQueue = folly::coro::BoundedQueue<std::optional<BuiltBatch>>;

  /// Concurrent stage layout (executor domains shown on the right):
  ///
  ///   +----------------------+                                [I/O executor]
  ///   | AsyncConsumerQueue   | --next_batch--> fetch_loop
  ///   +----------------------+
  ///                 |
  ///                 | enqueue(FetchedBatch)
  ///                 v
  ///          +----------------+                               [shared queue]
  ///          | fetched_queue_ |
  ///          +----------------+
  ///                 |
  ///                 | dequeue (N workers in parallel)
  ///                 v
  ///      +--------------------+                              [CPU executor]
  ///      | build_loop workers | --build_batch--> BuiltBatch
  ///      +--------------------+
  ///                 |
  ///                 | enqueue(BuiltBatch)
  ///                 v
  ///           +--------------+                               [shared queue]
  ///           | built_queue_ |
  ///           +--------------+
  ///                 |
  ///                 | await_task/process_task
  ///                 v
  ///      ordered   : sequence barrier (`ordered_batches_`) -> in-order push
  ///      unordered : push as soon as any worker finishes
  ///
  /// Invariants:
  /// 1. Backpressure is bounded by both `fetched_queue_` capacity and
  ///    `prefetch_bytes` accounting (`in_flight_fetch_bytes_`).
  /// 2. `seq` is assigned in fetch order; only ordered mode re-establishes
  ///    that order before emitting slices.
  /// 3. Fetch polling (`fetch_wait_timeout`) is independent from slice
  ///    flush latency (`batch_timeout`) to avoid timeout-coupling stalls.
  /// 4. Source ingress is typically the bottleneck. Raising worker concurrency
  ///    helps mostly in `optimization="unordered"` mode but does not remove
  ///    fetch-side wait on its own.

  /// Adds `delta` to one instrumentation counter when perf stats are enabled.
  auto add_perf_counter(std::atomic<uint64_t>& counter, uint64_t delta
                                                        = 1) const -> void {
    if (not perf_enabled_) {
      return;
    }
    counter.fetch_add(delta, std::memory_order_relaxed);
  }

  /// Emits a one-time, compact stage timing summary for benchmarking runs.
  auto emit_perf_summary(const char* reason) const -> void {
    if (not perf_enabled_ or not perf_started_) {
      return;
    }
    auto expected = false;
    if (not perf_reported_.compare_exchange_strong(expected, true,
                                                   std::memory_order_relaxed)) {
      return;
    }
    auto load = [](const std::atomic<uint64_t>& counter) {
      return counter.load(std::memory_order_relaxed);
    };
    auto elapsed_ns = std::max<uint64_t>(
      1, as_ns(std::chrono::steady_clock::now() - perf_start_));
    auto elapsed_s = static_cast<double>(elapsed_ns) / 1'000'000'000.0;
    auto emitted_messages = load(perf_.emitted_messages);
    auto eps = static_cast<double>(emitted_messages) / elapsed_s;
    TENZIR_WARN(
      "from_kafka perf: reason={} topic={} elapsed_ms={} eps={:.0f} "
      "emitted_messages={} emitted_batches={} emitted_slices={} "
      "fetch_calls={} fetch_wait_ms={} fetch_timeouts={} fetched_batches={} "
      "fetched_messages={} fetched_mb={:.2f} prefetch_wait_ms={} "
      "fetched_enqueue_wait_ms={} build_dequeue_wait_ms_total={} "
      "build_compute_ms_total={} build_enqueue_wait_ms_total={} "
      "built_batches={} built_messages={} "
      "runner_dequeue_wait_ms={} push_wait_ms={} eof_events={} fatal_errors={}",
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
      load(perf_.fatal_errors));
  }

  /// Computes the configured worker-side batch size.
  auto resolve_worker_batch_size() const -> size_t {
    auto batch_size = args_.worker_batch_size;
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
    auto concurrency = args_.worker_concurrency;
    if (concurrency == 0) {
      concurrency = default_worker_concurrency();
    }
    if (concurrency == 0) {
      concurrency = 1;
    }
    return static_cast<size_t>(concurrency);
  }

  /// Returns the next fetch-batch cap, honoring any `count=` limit.
  auto fetch_batch_size_limit() const -> size_t {
    auto limit = static_cast<uint64_t>(worker_batch_size_);
    if (args_.count) {
      if (scheduled_messages_ >= args_.count->inner) {
        return 0;
      }
      auto remaining = args_.count->inner - scheduled_messages_;
      limit = std::min(limit, remaining);
    }
    if (limit == 0) {
      limit = 1;
    }
    return static_cast<size_t>(limit);
  }

  /// Requests all pipeline stages to stop and wakes budget waiters.
  auto request_pipeline_stop() const -> void {
    if (pipeline_stop_requested_.exchange(true)) {
      return;
    }
    if (queue_) {
      (*queue_)->request_stop();
    }
    prefetch_budget_notify_.notify_one();
  }

  /// Returns whether any pipeline stage requested shutdown.
  auto is_pipeline_stopping() const -> bool {
    return pipeline_stop_requested_.load();
  }

  /// Waits until enough byte budget is available for one fetched batch.
  auto acquire_prefetch_budget(size_t bytes) const -> Task<void> {
    if (bytes == 0) {
      co_return;
    }
    while (not is_pipeline_stopping()) {
      {
        auto guard = std::scoped_lock{prefetch_budget_mutex_};
        // Never block forever on one oversized fetch batch. Allow exactly one
        // oversized in-flight batch at a time when no other batch is tracked.
        if (bytes > args_.prefetch_bytes) {
          if (in_flight_fetch_bytes_ == 0) {
            in_flight_fetch_bytes_ = bytes;
            co_return;
          }
        } else if (in_flight_fetch_bytes_ + bytes <= args_.prefetch_bytes) {
          in_flight_fetch_bytes_ += bytes;
          co_return;
        }
      }
      co_await prefetch_budget_notify_.wait();
    }
  }

  /// Releases byte budget after worker-side processing completes.
  auto release_prefetch_budget(size_t bytes) const noexcept -> void {
    if (bytes == 0) {
      return;
    }
    {
      auto guard = std::scoped_lock{prefetch_budget_mutex_};
      if (bytes >= in_flight_fetch_bytes_) {
        in_flight_fetch_bytes_ = 0;
      } else {
        in_flight_fetch_bytes_ -= bytes;
      }
    }
    prefetch_budget_notify_.notify_one();
  }

  /// Enqueues exactly one shutdown sentinel per builder worker.
  auto close_fetched_queue() const -> Task<void> {
    if (not fetched_queue_) {
      co_return;
    }
    auto expected = false;
    if (not fetched_queue_closed_.compare_exchange_strong(expected, true)) {
      co_return;
    }
    for (size_t i = 0; i < worker_count_; ++i) {
      co_await fetched_queue_->enqueue(std::nullopt);
    }
  }

  /// Converts consumed Kafka messages into one fetch-stage payload batch.
  auto to_fetched_batch(
    std::vector<AsyncConsumerQueue::Message> fetched_messages) const
    -> std::optional<FetchedBatch> {
    auto batch = FetchedBatch{};
    batch.messages.reserve(fetched_messages.size());
    auto reached_count = false;
    for (auto& message : fetched_messages) {
      switch (message.err()) {
        case RD_KAFKA_RESP_ERR_NO_ERROR: {
          batch.payload_bytes += message.len();
          batch.messages.push_back(std::move(message));
          ++scheduled_messages_;
          if (args_.count and scheduled_messages_ >= args_.count->inner) {
            reached_count = true;
          }
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
        and not batch.fatal_error) {
      return std::nullopt;
    }
    batch.seq = next_fetch_seq_++;
    return batch;
  }

  /// Fetches Kafka batches and hands them to worker coroutines.
  auto fetch_loop() const -> Task<void> {
    if (not queue_ or not fetched_queue_) {
      co_return;
    }
    try {
      while (not is_pipeline_stopping()) {
        auto max_messages = fetch_batch_size_limit();
        if (max_messages == 0) {
          request_pipeline_stop();
          break;
        }
        auto pending_messages = std::vector<AsyncConsumerQueue::Message>{};
        pending_messages.reserve(max_messages);
        auto min_wait = std::chrono::duration_cast<duration>(1ms);
        auto base_poll_wait = std::max(args_.fetch_wait_timeout, min_wait);
        auto poll_wait = base_poll_wait;
        // Backoff invariant: only idle (empty) timeout streaks increase
        // `poll_wait`. Any fetched message resets to `base_poll_wait`.
        auto poll_wait_cap
          = std::max(base_poll_wait, std::chrono::duration_cast<duration>(
                                       fetch_wait_backoff_cap));
        auto consecutive_empty_timeouts = size_t{0};
        auto batch_deadline
          = std::optional<std::chrono::steady_clock::time_point>{};
        while (pending_messages.size() < max_messages
               and not is_pipeline_stopping()) {
          auto wait
            = std::chrono::duration_cast<std::chrono::milliseconds>(poll_wait);
          if (not pending_messages.empty()) {
            TENZIR_ASSERT(batch_deadline);
            auto now = std::chrono::steady_clock::now();
            if (now >= *batch_deadline) {
              break;
            }
            auto remaining = *batch_deadline - now;
            wait = std::chrono::duration_cast<std::chrono::milliseconds>(
              std::min<std::chrono::steady_clock::duration>(remaining,
                                                            poll_wait));
            if (wait <= 0ms) {
              wait = 1ms;
            }
          }
          auto next_batch_started = std::chrono::steady_clock::time_point{};
          if (perf_enabled_) {
            add_perf_counter(perf_.fetch_next_batch_calls);
            next_batch_started = std::chrono::steady_clock::now();
          }
          auto result = co_await (*queue_)->next_batch(
            max_messages - pending_messages.size(), wait);
          if (perf_enabled_) {
            add_perf_counter(
              perf_.fetch_next_batch_wait_ns,
              as_ns(std::chrono::steady_clock::now() - next_batch_started));
          }
          if (result.messages.empty()) {
            if (result.timed_out) {
              add_perf_counter(perf_.fetch_timeouts);
              if (not pending_messages.empty()) {
                break;
              }
              ++consecutive_empty_timeouts;
              if (consecutive_empty_timeouts >= fetch_wait_backoff_after) {
                poll_wait = std::min(poll_wait_cap, poll_wait * 2);
              }
              continue;
            }
            break;
          }
          consecutive_empty_timeouts = 0;
          poll_wait = base_poll_wait;
          std::ranges::move(result.messages,
                            std::back_inserter(pending_messages));
          if (not batch_deadline) {
            batch_deadline = std::chrono::steady_clock::now()
                             + std::max(args_.batch_timeout, min_wait);
          }
        }
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
        if (perf_enabled_) {
          add_perf_counter(perf_.fetched_batches);
          add_perf_counter(perf_.fetched_messages,
                           static_cast<uint64_t>(fetched->messages.size()));
          add_perf_counter(perf_.fetched_payload_bytes, fetched->payload_bytes);
        }
        auto reserved_bytes = fetched->payload_bytes;
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
            break;
          }
        }
        auto release_budget = tenzir::detail::scope_guard{
          [this, reserved_bytes]() noexcept {
            release_prefetch_budget(reserved_bytes);
          },
        };
        auto has_fatal = fetched->fatal_error.has_value();
        auto reached_count = fetched->reached_count;
        auto enqueue_started = std::chrono::steady_clock::time_point{};
        if (perf_enabled_) {
          enqueue_started = std::chrono::steady_clock::now();
        }
        co_await fetched_queue_->enqueue(
          std::optional<FetchedBatch>{std::move(*fetched)});
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
          break;
        }
        if (is_pipeline_stopping()) {
          break;
        }
      }
    } catch (const folly::OperationCancelled&) {
      request_pipeline_stop();
    }
    co_await close_fetched_queue();
  }

  /// Converts one fetched payload batch into a `table_slice`.
  auto build_batch(FetchedBatch fetched) const -> BuiltBatch {
    auto built = BuiltBatch{};
    built.seq = fetched.seq;
    built.eof_partitions = std::move(fetched.eof_partitions);
    built.fatal_error = std::move(fetched.fatal_error);
    if (fetched.messages.empty()) {
      return built;
    }
    auto builder = KafkaMessageBuilder{};
    for (auto& message : fetched.messages) {
      auto payload_bytes = message.payload();
      if (payload_bytes.is_err()) {
        built.fatal_error = fmt::format("invalid kafka payload in partition {} "
                                        "at offset {}: {}",
                                        message.partition(), message.offset(),
                                        std::move(payload_bytes).unwrap_err());
        break;
      }
      builder.append(std::move(payload_bytes).unwrap());
      built.max_offsets[message.partition()] = message.offset();
      ++built.message_count;
    }
    if (not builder.empty()) {
      built.slice = builder.finish();
    }
    return built;
  }

  /// Runs one CPU-stage worker that builds slices from fetched batches.
  auto build_loop() const -> Task<void> {
    if (not fetched_queue_ or not built_queue_) {
      co_return;
    }
    try {
      while (true) {
        auto dequeue_started = std::chrono::steady_clock::time_point{};
        if (perf_enabled_) {
          dequeue_started = std::chrono::steady_clock::now();
        }
        auto next = co_await fetched_queue_->dequeue();
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
        auto built = build_batch(std::move(fetched));
        if (perf_enabled_) {
          add_perf_counter(
            perf_.build_compute_ns,
            as_ns(std::chrono::steady_clock::now() - build_started));
          add_perf_counter(perf_.built_batches);
          add_perf_counter(perf_.built_messages,
                           static_cast<uint64_t>(built.message_count));
        }
        release_budget.trigger();
        auto enqueue_started = std::chrono::steady_clock::time_point{};
        if (perf_enabled_) {
          enqueue_started = std::chrono::steady_clock::now();
        }
        co_await built_queue_->enqueue(std::optional<BuiltBatch>{
          std::move(built),
        });
        if (perf_enabled_) {
          add_perf_counter(
            perf_.build_enqueue_wait_ns,
            as_ns(std::chrono::steady_clock::now() - enqueue_started));
        }
      }
    } catch (const folly::OperationCancelled&) {
      request_pipeline_stop();
    }
    if (live_builders_.fetch_sub(1) == 1 and built_queue_) {
      co_await built_queue_->enqueue(std::nullopt);
    }
  }

  /// Waits for the next in-order built batch from worker output.
  auto await_ordered_batch() const -> Task<FromKafkaTaskResult> {
    TENZIR_ASSERT(built_queue_);
    while (true) {
      auto ready = ordered_batches_.find(next_emit_seq_);
      if (ready != ordered_batches_.end()) {
        auto batch = std::move(ready->second);
        ordered_batches_.erase(ready);
        ++next_emit_seq_;
        co_return FromKafkaTaskResult{
          .batch = std::move(batch),
        };
      }
      auto dequeue_started = std::chrono::steady_clock::time_point{};
      if (perf_enabled_) {
        dequeue_started = std::chrono::steady_clock::now();
      }
      auto next = co_await built_queue_->dequeue();
      if (perf_enabled_) {
        add_perf_counter(
          perf_.runner_dequeue_wait_ns,
          as_ns(std::chrono::steady_clock::now() - dequeue_started));
      }
      if (not next) {
        if (ordered_batches_.empty()) {
          co_return FromKafkaTaskResult{.end_of_stream = true};
        }
        auto contiguous = ordered_batches_.find(next_emit_seq_);
        TENZIR_ASSERT(contiguous != ordered_batches_.end());
        continue;
      }
      if (next->seq == next_emit_seq_) {
        ++next_emit_seq_;
        co_return FromKafkaTaskResult{
          .batch = std::move(*next),
        };
      }
      auto [_, inserted]
        = ordered_batches_.emplace(next->seq, std::move(*next));
      TENZIR_ASSERT(inserted);
    }
  }

  /// Moves per-partition offsets from one emitted batch into checkpoint state.
  auto advance_checkpoint(std::unordered_map<int32_t, int64_t> const& offsets)
    -> void {
    for (const auto& [partition, offset] : offsets) {
      checkpoint_pending_offsets_[partition] = offset + 1;
    }
  }

  /// Tracks partition EOF notifications and marks the source done if complete.
  auto mark_partition_eof(int32_t partition, OpCtx& ctx) -> void {
    if (not args_.exit or not consumer_) {
      return;
    }
    auto partitions = std::vector<RdKafka::TopicPartition*>{};
    if (auto err = (*consumer_)->assignment(partitions);
        err != RdKafka::ERR_NO_ERROR) {
      diagnostic::error("failed to get assignment: {}", RdKafka::err2str(err))
        .emit(ctx);
      done_ = true;
      return;
    }
    auto assignment = std::unordered_set<int32_t>{};
    for (auto* candidate : partitions) {
      if (candidate and candidate->topic() == args_.topic) {
        assignment.insert(candidate->partition());
      }
    }
    RdKafka::TopicPartition::destroy(partitions);
    if (assignment.empty()) {
      return;
    }
    if (not assigned_partitions_ or *assigned_partitions_ != assignment) {
      assigned_partitions_ = std::move(assignment);
      eof_partitions_.clear();
    }
    if (not assigned_partitions_->contains(partition)) {
      return;
    }
    eof_partitions_.insert(partition);
    if (eof_partitions_.size() == assigned_partitions_->size()) {
      done_ = true;
    }
  }

  FromKafkaArgs args_;
  OptimizationMode optimization_mode_ = OptimizationMode::ordered;
  size_t worker_count_ = 1;
  size_t worker_batch_size_ = 1;
  std::optional<configuration> cfg_;
  std::optional<Box<RdKafka::KafkaConsumer>> consumer_;
  mutable std::optional<Box<AsyncConsumerQueue>> queue_;
  std::shared_ptr<FetchedQueue> fetched_queue_;
  std::shared_ptr<BuiltQueue> built_queue_;
  mutable std::map<uint64_t, BuiltBatch> ordered_batches_;
  mutable std::atomic<bool> fetched_queue_closed_ = false;
  mutable std::atomic<size_t> live_builders_ = 0;
  mutable std::atomic<bool> pipeline_stop_requested_ = false;
  mutable std::mutex prefetch_budget_mutex_;
  mutable size_t in_flight_fetch_bytes_ = 0;
  mutable Notify prefetch_budget_notify_;
  mutable uint64_t next_fetch_seq_ = 0;
  mutable uint64_t next_emit_seq_ = 0;
  mutable uint64_t scheduled_messages_ = 0;
  // Number of records emitted so far; used for `count=` resumption.
  size_t emitted_messages_ = 0;
  // Invariant: committed offsets are stored as "next offset to consume".
  std::unordered_map<int32_t, int64_t> checkpoint_pending_offsets_;
  // Invariant: when set, this is the latest assignment for `args_.topic`.
  std::optional<std::unordered_set<int32_t>> assigned_partitions_;
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
class FromKafkaPlugin final : public virtual OperatorPlugin {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
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
      const auto* plugin_config = try_as<record>(&ptr->second);
      if (not plugin_config) {
        return;
      }
      auto kafka_config_ptr = plugin_config->find("kafka");
      if (kafka_config_ptr == plugin_config->end()) {
        return;
      }
      const auto* kafka_config = try_as<record>(&kafka_config_ptr->second);
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

  auto describe() const -> Description override {
    auto initial = FromKafkaArgs{};
    initial.options = located{record{}, location::unknown};
    auto d = Describer<FromKafkaArgs, FromKafkaOperator>{std::move(initial)};
    d.positional("topic", &FromKafkaArgs::topic);
    d.named("count", &FromKafkaArgs::count);
    auto exit_arg = d.named("exit", &FromKafkaArgs::exit);
    auto offset_arg = d.named("offset", &FromKafkaArgs::offset);
    auto optimization_arg
      = d.named_optional("optimization", &FromKafkaArgs::optimization);
    auto options_arg = d.named_optional("options", &FromKafkaArgs::options);
    auto aws_region_arg = d.named("aws_region", &FromKafkaArgs::aws_region);
    auto aws_iam_arg = d.named("aws_iam", &FromKafkaArgs::aws_iam);
    auto batch_size_arg
      = d.named_optional("batch_size", &FromKafkaArgs::batch_size);
    auto worker_batch_size_arg = d.named_optional(
      "worker_batch_size", &FromKafkaArgs::worker_batch_size);
    auto worker_concurrency_arg = d.named_optional(
      "worker_concurrency", &FromKafkaArgs::worker_concurrency);
    auto prefetch_batches_arg
      = d.named_optional("prefetch_batches", &FromKafkaArgs::prefetch_batches);
    auto prefetch_bytes_arg
      = d.named_optional("prefetch_bytes", &FromKafkaArgs::prefetch_bytes);
    auto batch_timeout_arg
      = d.named_optional("batch_timeout", &FromKafkaArgs::batch_timeout);
    auto fetch_wait_timeout_arg = d.named_optional(
      "fetch_wait_timeout", &FromKafkaArgs::fetch_wait_timeout);
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto mode = OptimizationMode::ordered;
      if (auto optimization = ctx.get(optimization_arg); optimization) {
        auto parsed = from_string<OptimizationMode>(*optimization);
        if (not parsed) {
          diagnostic::error("invalid `optimization` value `{}`", *optimization)
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
          diagnostic::error("`worker_batch_size` must be greater than zero")
            .primary(ctx.get_location(worker_batch_size_arg)
                       .value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto worker_concurrency = ctx.get(worker_concurrency_arg);
          worker_concurrency) {
        if (*worker_concurrency == 0) {
          diagnostic::error("`worker_concurrency` must be greater than zero")
            .primary(ctx.get_location(worker_concurrency_arg)
                       .value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto prefetch_batches = ctx.get(prefetch_batches_arg);
          prefetch_batches) {
        if (*prefetch_batches == 0) {
          diagnostic::error("`prefetch_batches` must be greater than zero")
            .primary(
              ctx.get_location(prefetch_batches_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto prefetch_bytes = ctx.get(prefetch_bytes_arg); prefetch_bytes) {
        if (*prefetch_bytes == 0) {
          diagnostic::error("`prefetch_bytes` must be greater than zero")
            .primary(
              ctx.get_location(prefetch_bytes_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto batch_timeout = ctx.get(batch_timeout_arg); batch_timeout) {
        if (*batch_timeout <= duration::zero()) {
          diagnostic::error("`batch_timeout` must be a positive duration")
            .primary(
              ctx.get_location(batch_timeout_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto fetch_wait_timeout = ctx.get(fetch_wait_timeout_arg);
          fetch_wait_timeout) {
        if (*fetch_wait_timeout <= duration::zero()) {
          diagnostic::error("`fetch_wait_timeout` must be a positive duration")
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
            .primary(iam->source)
            .emit(ctx);
          return {};
        }
      }
      if (mode == OptimizationMode::unordered) {
        if (ctx.get(exit_arg)) {
          diagnostic::error(
            "`optimization=\"unordered\"` is incompatible with `exit`")
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
