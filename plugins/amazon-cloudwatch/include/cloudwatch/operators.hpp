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
#include <tenzir/fwd.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/result.hpp>
#include <tenzir/tql2/ast.hpp>

#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace tenzir::plugins::cloudwatch {

enum class FromMode {
  live,
  search,
  replay,
};

enum class ToMethod {
  put,
  hlc,
  ndjson,
  json,
};

struct FromCloudWatchArgs {
  located<data> group;
  located<std::string> mode = {"live", location::unknown};
  Option<located<data>> stream;
  Option<located<std::string>> stream_prefix;
  Option<located<std::string>> filter;
  Option<located<time>> start;
  Option<located<time>> end;
  Option<located<uint64_t>> count;
  Option<located<bool>> from_start;
  Option<located<bool>> unmask;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  location operator_location;
};

struct ToCloudWatchArgs {
  located<std::string> log_group;
  Option<located<std::string>> log_stream;
  located<std::string> method = {"put", location::unknown};
  ast::expression payload;
  ast::expression timestamp;
  Option<located<duration>> batch_timeout;
  Option<located<uint64_t>> batch_size;
  Option<located<uint64_t>> parallel;
  Option<located<secret>> token;
  Option<located<std::string>> endpoint;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  location operator_location;
};

struct CloudWatchEvent {
  time timestamp;
  time ingestion_time;
  std::string log_group;
  std::string log_stream;
  std::string message;
  Option<std::string> event_id;
};

/// A page of events read from CloudWatch Logs.
struct SourcePage {
  std::vector<CloudWatchEvent> events;
  std::string next_token;
  /// Whether this is the final page; no further reads should follow.
  bool done = false;
};

/// The outcome of a source read: either a page of events or a failure message.
using SourceResult = Result<SourcePage, std::string>;

enum class ToCloudWatchDiagnosticSeverity {
  warning,
  error,
};

enum class ToCloudWatchDiagnosticPrimary {
  operator_,
  payload,
};

struct ToCloudWatchDiagnostic {
  ToCloudWatchDiagnostic() = default;
  ToCloudWatchDiagnostic(ToCloudWatchDiagnosticSeverity severity,
                         ToCloudWatchDiagnosticPrimary primary,
                         std::string message,
                         std::vector<std::string> notes = {})
    : severity{severity},
      primary{primary},
      message{std::move(message)},
      notes{std::move(notes)} {
  }

  ToCloudWatchDiagnosticSeverity severity
    = ToCloudWatchDiagnosticSeverity::warning;
  ToCloudWatchDiagnosticPrimary primary
    = ToCloudWatchDiagnosticPrimary::operator_;
  std::string message;
  std::vector<std::string> notes;
};

struct ToCloudWatchSendReport {
  std::vector<ToCloudWatchDiagnostic> diagnostics;
  size_t bytes = 0;
  size_t events = 0;
  bool failed = false;
};

/// Wakeup messages delivered to `ToCloudWatch::await_task()` through the
/// wakeup queue.
struct ToCloudWatchFlushTimeout {};
struct ToCloudWatchReportReady {};

auto default_to_amazon_cloudwatch_message_expression() -> ast::expression;

class FromCloudWatch final : public Operator<void, table_slice> {
public:
  explicit FromCloudWatch(FromCloudWatchArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto snapshot(Serde& serde) -> void override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;
  auto state() -> OperatorState override;
  auto stop(OpCtx& ctx) -> Task<void> override;

private:
  FromCloudWatchArgs args_;
  FromMode mode_ = FromMode::live;
  std::shared_ptr<amazon::SignedHttpClient> client_;
  folly::CancellationSource live_cancel_;
  mutable Option<Arc<folly::coro::BoundedQueue<SourceResult, false, true>>>
    live_queue_;
  std::string next_token_;
  uint64_t emitted_ = 0;
  bool done_ = false;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
};

class ToCloudWatch final : public Operator<table_slice, void> {
public:
  explicit ToCloudWatch(ToCloudWatchArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(table_slice input, OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, OpCtx& ctx) -> Task<void> override;
  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;
  auto state() -> OperatorState override;

public:
  struct Event {
    time timestamp;
    std::string message;
  };

private:
  auto flush(OpCtx& ctx) -> Task<void>;
  auto wait_for_requests(OpCtx& ctx) -> Task<void>;
  auto handle_send_report(ToCloudWatchSendReport report, OpCtx& ctx) -> void;
  auto drain_send_reports(OpCtx& ctx) -> void;

  /// Spawns a task that sleeps until the current batch deadline and then
  /// enqueues a `ToCloudWatchFlushTimeout` wakeup.
  auto arm_flush_timer(OpCtx& ctx) -> void;

  ToCloudWatchArgs args_;
  ToMethod method_ = ToMethod::put;
  std::shared_ptr<amazon::SignedHttpClient> client_;
  std::string token_;
  std::vector<Event> batch_;
  uint64_t batch_size_ = 1000;
  duration batch_timeout_ = std::chrono::seconds{1};
  Option<std::chrono::steady_clock::time_point> next_timeout_;
  /// Whether a flush-timer task is outstanding. Bounds the number of live
  /// timer tasks to one; a timer that fires for an already-flushed batch
  /// re-arms itself for the deadline of the batch that replaced it.
  bool timer_armed_ = false;
  /// Wakeup messages for `await_task()`. Helper tasks enqueue, only the
  /// operator driver dequeues and updates state; operator members are never
  /// touched from concurrently running tasks. The capacity only bounds
  /// buffering: a full queue suspends the producing helper task until the
  /// driver drains it.
  mutable Arc<folly::coro::BoundedQueue<Any>> wakeup_queue_{std::in_place, 16};
  mutable Option<Arc<folly::coro::BoundedQueue<ToCloudWatchSendReport>>>
    send_queue_;
  uint64_t parallel_ = 1;
  uint64_t pending_reports_ = 0;
  Semaphore request_slots_{1};
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
  bool done_ = false;
};

} // namespace tenzir::plugins::cloudwatch
