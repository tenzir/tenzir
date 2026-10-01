//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/amazon.hpp>
#include <tenzir/arc.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/data.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/result.hpp>
#include <tenzir/tql2/ast.hpp>

#include <folly/coro/BoundedQueue.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace tenzir::plugins::amazon_kinesis {

struct KinesisApiError {
  KinesisApiError() = default;
  explicit KinesisApiError(std::string message, std::string code = {})
    : message{std::move(message)}, code{std::move(code)} {
  }

  std::string message;
  std::string code;
};

struct FromAmazonKinesisArgs {
  located<std::string> stream;
  Option<located<data>> start;
  Option<located<uint64_t>> count;
  bool exit = false;
  Option<located<uint64_t>> records_per_call;
  Option<located<duration>> poll_idle;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  Option<located<std::string>> endpoint;
  location operator_location;
};

struct ToAmazonKinesisArgs {
  located<std::string> stream;
  ast::expression message;
  Option<ast::expression> partition_key;
  Option<located<uint64_t>> batch_size;
  Option<located<duration>> batch_timeout;
  Option<located<uint64_t>> parallel;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  Option<located<std::string>> endpoint;
  location operator_location;
};

auto default_message_expression() -> ast::expression;

auto make_kinesis_http_client(Option<located<std::string>> const& aws_region,
                              Option<located<record>> const& aws_iam,
                              Option<located<std::string>> const& endpoint,
                              OpCtx& ctx)
  -> Task<std::shared_ptr<amazon::SignedHttpClient>>;

/// A record read from a Kinesis shard.
struct ReadRecord {
  blob message;
  std::string stream;
  std::string shard_id;
  std::string sequence_number;
  std::string partition_key;
  Option<time> arrival_time;
  std::string encryption_type;
  duration behind_latest = {};
};

/// A record waiting to be written to a Kinesis stream.
struct PendingRecord {
  blob message;
  std::string partition_key;
};

/// Reads the shards of a stream for `from_amazon_kinesis`, independent of the
/// event representation.
class KinesisReader {
public:
  explicit KinesisReader(FromAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void>;

  /// Waits for the next report of a shard loop.
  auto next() const -> Task<Any>;

  /// Applies a report from `next()` and returns the records to emit.
  auto handle(Any result, OpCtx& ctx) -> Task<std::vector<ReadRecord>>;

  /// Accounts for `count` emitted events.
  auto count_events(uint64_t count) -> void;

  auto state() -> OperatorState;
  auto snapshot(Serde& serde) -> void;

private:
  struct ShardState {
    std::string id;
    std::vector<std::string> parents;
    std::string next_sequence_number;
    Option<time> latest_start_time;
    bool trim_horizon_start = false;
    bool closed = false;
    /// Whether the shard's loop finished because it caught up with the
    /// stream tip in exit mode. Monotone: only ever set by the shard's own
    /// final result, so it can never go stale while a read is in flight.
    bool caught_up = false;
    /// Whether a `shard_loop()` task is currently reading this shard.
    /// Transient runtime state, deliberately excluded from `inspect()` so
    /// that restored shards respawn their loops.
    bool loop_running = false;

    friend auto inspect(auto& f, ShardState& x) -> bool {
      return f.object(x).fields(
        f.field("id", x.id), f.field("parents", x.parents),
        f.field("next_sequence_number", x.next_sequence_number),
        f.field("latest_start_time", x.latest_start_time),
        f.field("trim_horizon_start", x.trim_horizon_start),
        f.field("closed", x.closed), f.field("caught_up", x.caught_up));
    }
  };

  auto recreate_shard_iterator(const ShardState& shard)
    -> Task<Result<std::string, KinesisApiError>>;

  /// Reads one shard until it closes or fails, enqueueing results.
  auto shard_loop(ShardState shard) -> Task<void>;

  /// Spawns loops for shards without a running loop and whose parents are all
  /// closed, preserving parent-before-child ordering.
  auto spawn_ready_loops(OpCtx& ctx) -> void;

  auto parents_closed(const ShardState& shard) const -> bool;

  auto discover_new_shards(OpCtx& ctx) -> Task<void>;

  /// Whether every shard has either closed or caught up with the stream tip,
  /// which is the exit-mode termination condition.
  auto all_shards_finished() const -> bool;

  FromAmazonKinesisArgs args_;
  std::shared_ptr<amazon::SignedHttpClient> client_;
  std::vector<ShardState> shards_;
  uint64_t emitted_ = 0;
  uint64_t limit_ = 0;
  int records_per_call_ = 1000;
  duration poll_idle_ = std::chrono::seconds{1};
  bool done_ = false;
  mutable Box<folly::coro::BoundedQueue<Any>> results_{std::in_place, 16};
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
};

/// Batches and writes records to a stream for `to_amazon_kinesis`,
/// independent of the event representation.
class KinesisWriter {
public:
  /// The outcome of one asynchronous PutRecords send.
  struct SendReport {
    std::vector<PendingRecord> failed_records;
    std::vector<std::string> errors;
    size_t bytes = 0;
    size_t events = 0;
  };

  /// Wakeup messages delivered to `next()` through `wakeup_queue_`.
  struct ReportReady {};
  struct FlushTimeout {};

  explicit KinesisWriter(ToAmazonKinesisArgs const& args);

  auto start(OpCtx& ctx) -> Task<void>;

  /// Whether the writer failed and drops all further records.
  auto failed() const -> bool;

  /// Adds a record to the current batch, using a random partition key if
  /// `partition_key` is `None`. Skips the record with a warning if it is
  /// invalid.
  auto add(blob message, Option<std::string> partition_key, OpCtx& ctx)
    -> Task<void>;

  /// Waits for the next wakeup message.
  auto next() const -> Task<Any>;

  /// Handles a wakeup message from `next()`.
  auto handle(Any result, OpCtx& ctx) -> Task<void>;

  /// Sends all records and waits for the pending requests.
  auto flush_all(OpCtx& ctx) -> Task<void>;

  auto state() -> OperatorState;

private:
  auto flush_if_timed_out(OpCtx& ctx) -> Task<void>;
  auto flush(OpCtx& ctx) -> Task<void>;

  /// Spawns a task that sleeps until the current batch deadline and then
  /// enqueues a `FlushTimeout` wakeup.
  auto arm_flush_timer(OpCtx& ctx) -> void;

  auto handle_send_report(SendReport report, OpCtx& ctx) -> void;
  auto drain_send_reports(OpCtx& ctx) -> void;
  auto wait_for_requests(OpCtx& ctx) -> Task<void>;
  auto fail_if_unsent(OpCtx& ctx) -> void;

  located<std::string> stream_;
  location message_location_;
  Option<location> partition_key_location_;
  Option<located<std::string>> aws_region_;
  Option<located<record>> aws_iam_;
  Option<located<std::string>> endpoint_;
  std::shared_ptr<amazon::SignedHttpClient> client_;
  std::vector<PendingRecord> batch_;
  size_t batch_size_ = 500;
  /// The stream's configured record size limit, discovered at startup.
  size_t max_record_size_ = 10 * 1024 * 1024;
  duration batch_timeout_ = std::chrono::seconds{1};
  Option<std::chrono::steady_clock::time_point> batch_deadline_ = None{};
  /// Whether a flush-timer task is outstanding. Bounds the number of live
  /// timer tasks to one; a timer that fires for an already-flushed batch
  /// re-arms itself for the deadline of the batch that replaced it.
  bool timer_armed_ = false;
  /// Wakeup messages for `next()`. Helper tasks enqueue, only the operator
  /// driver dequeues and updates state; members are never touched from
  /// concurrently running tasks. Queue capacities just bound buffering: a
  /// full queue suspends the producing helper task until the driver drains
  /// it.
  mutable Arc<folly::coro::BoundedQueue<Any>> wakeup_queue_{std::in_place, 16};
  Arc<folly::coro::BoundedQueue<SendReport>> send_queue_{std::in_place, 16};
  Semaphore request_slots_{1};
  uint64_t pending_reports_ = 0;
  bool failed_ = false;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
};

class FromAmazonKinesis final : public Operator<void, table_slice> {
public:
  explicit FromAmazonKinesis(FromAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;
  auto state() -> OperatorState override;
  auto snapshot(Serde& serde) -> void override;

private:
  KinesisReader reader_;
};

class FromAmazonKinesisEvents final : public Operator<void, nova::Events> {
public:
  explicit FromAmazonKinesisEvents(FromAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override;
  auto state() -> OperatorState override;
  auto snapshot(Serde& serde) -> void override;

private:
  KinesisReader reader_;
};

class ToAmazonKinesis final : public Operator<table_slice, void> {
public:
  explicit ToAmazonKinesis(ToAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(table_slice input, OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, OpCtx& ctx) -> Task<void> override;
  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;
  auto state() -> OperatorState override;

private:
  ToAmazonKinesisArgs args_;
  KinesisWriter writer_;
};

class ToAmazonKinesisEvents final : public Operator<nova::Events, void> {
public:
  explicit ToAmazonKinesisEvents(ToAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, OpCtx& ctx) -> Task<void> override;
  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;
  auto state() -> OperatorState override;

private:
  ToAmazonKinesisArgs args_;
  KinesisWriter writer_;
  Option<nova::Evaluator> message_;
  Option<nova::Evaluator> partition_key_;
};

} // namespace tenzir::plugins::amazon_kinesis
