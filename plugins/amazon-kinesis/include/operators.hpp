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
#include <tenzir/async/notify.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/data.hpp>
#include <tenzir/fwd.hpp>
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
  struct ShardState {
    std::string id;
    std::vector<std::string> parents;
    std::string next_sequence_number;
    Option<time> latest_start_time;
    bool trim_horizon_start = false;
    bool closed = false;
    bool idle = false;

    friend auto inspect(auto& f, ShardState& x) -> bool {
      return f.object(x).fields(
        f.field("id", x.id), f.field("parents", x.parents),
        f.field("next_sequence_number", x.next_sequence_number),
        f.field("latest_start_time", x.latest_start_time),
        f.field("trim_horizon_start", x.trim_horizon_start),
        f.field("closed", x.closed), f.field("idle", x.idle));
    }
  };

  auto recreate_shard_iterator(const ShardState& shard)
    -> Task<Result<std::string, KinesisApiError>>;

  /// Reads one shard until it closes or fails, enqueueing results.
  auto shard_loop(ShardState shard) -> Task<void>;

  /// Spawns loops for shards that have no running loop and whose parents are
  /// all closed, preserving parent-before-child ordering.
  auto spawn_ready_loops(OpCtx& ctx) -> void;

  auto parents_closed(const ShardState& shard) const -> bool;

  auto discover_new_shards(OpCtx& ctx) -> Task<void>;

  FromAmazonKinesisArgs args_;
  std::shared_ptr<amazon::SignedHttpClient> client_;
  std::vector<ShardState> shards_;
  std::vector<std::string> running_;
  /// The number of enqueued but unprocessed read results, to prevent `exit`
  /// from triggering while records are still in flight.
  uint64_t pending_results_ = 0;
  uint64_t emitted_ = 0;
  uint64_t limit_ = 0;
  int records_per_call_ = 1000;
  duration poll_idle_ = std::chrono::seconds{1};
  bool done_ = false;
  mutable Box<folly::coro::BoundedQueue<Any>> results_{std::in_place, 16};
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
};

class ToAmazonKinesis final : public Operator<table_slice, void> {
public:
  struct PendingRecord {
    blob message;
    std::string partition_key;
  };

  /// The outcome of one asynchronous PutRecords send.
  struct SendReport {
    std::vector<PendingRecord> failed_records;
    std::vector<std::string> errors;
    size_t bytes = 0;
    size_t events = 0;
  };

  /// Wakeup markers returned by `await_task()`.
  struct ReportReady {};
  struct FlushTimeout {};

  explicit ToAmazonKinesis(ToAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(table_slice input, OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, OpCtx& ctx) -> Task<void> override;
  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;
  auto state() -> OperatorState override;

private:
  auto flush_if_timed_out(OpCtx& ctx) -> Task<void>;
  auto flush(OpCtx& ctx) -> Task<void>;
  auto handle_send_report(SendReport report, OpCtx& ctx) -> void;
  auto drain_send_reports(OpCtx& ctx) -> void;
  auto wait_for_requests(OpCtx& ctx) -> Task<void>;
  auto fail_if_unsent(OpCtx& ctx) -> void;

  ToAmazonKinesisArgs args_;
  std::shared_ptr<amazon::SignedHttpClient> client_;
  std::vector<PendingRecord> batch_;
  size_t batch_size_ = 500;
  /// The stream's configured record size limit, discovered at startup.
  size_t max_record_size_ = 10 * 1024 * 1024;
  duration batch_timeout_ = std::chrono::seconds{1};
  mutable Option<std::chrono::steady_clock::time_point> batch_deadline_
    = None{};
  mutable Box<Notify> batch_ready_{std::in_place};
  mutable Arc<Notify> report_ready_{std::in_place};
  Option<Arc<folly::coro::BoundedQueue<SendReport>>> send_queue_;
  Semaphore request_slots_{1};
  uint64_t pending_reports_ = 0;
  uint64_t parallel_ = 1;
  bool failed_ = false;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
};

} // namespace tenzir::plugins::amazon_kinesis
