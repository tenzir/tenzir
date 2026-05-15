//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "cloudwatch/client.hpp"

#include <tenzir/async.hpp>
#include <tenzir/async/channel.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/tql2/ast.hpp>

#include <chrono>
#include <cstddef>
#include <string>
#include <vector>

namespace tenzir::plugins::cloudwatch {

enum class FromMode {
  live,
  filter,
  get,
};

enum class ToMethod {
  put_log_events,
  hlc,
};

struct FromCloudWatchArgs {
  located<std::string> log_group;
  located<std::string> mode = {"live", location::unknown};
  Option<located<data>> log_group_identifiers;
  Option<located<std::string>> log_stream;
  Option<located<data>> log_streams;
  Option<located<std::string>> log_stream_prefix;
  Option<located<std::string>> filter;
  Option<located<time>> start;
  Option<located<time>> end;
  Option<located<uint64_t>> limit;
  Option<located<bool>> start_from_head;
  Option<located<bool>> unmask;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  location operator_location;
};

struct ToCloudWatchArgs {
  located<std::string> log_group;
  located<std::string> log_stream;
  located<std::string> method = {"put_log_events", location::unknown};
  ast::expression message;
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

auto default_to_cloudwatch_message_expression() -> ast::expression;

class FromCloudWatch final : public Operator<void, table_slice> {
public:
  explicit FromCloudWatch(FromCloudWatchArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;
  auto state() -> OperatorState override;

private:
  FromCloudWatchArgs args_;
  FromMode mode_ = FromMode::live;
  Option<Arc<Aws::CloudWatchLogs::CloudWatchLogsClient>> client_;
  mutable Option<Box<HttpPool>> http_pool_;
  bool use_local_http_read_ = false;
  Option<Sender<Any>> live_tx_;
  mutable Option<Receiver<Any>> live_rx_;
  std::string next_token_;
  uint64_t emitted_ = 0;
  bool done_ = false;
  MetricsCounter bytes_read_counter_;
};

class ToCloudWatch final : public Operator<table_slice, void> {
public:
  explicit ToCloudWatch(ToCloudWatchArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(table_slice input, OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;

public:
  struct Event {
    time timestamp;
    std::string message;
  };

private:
  auto flush(OpCtx& ctx) -> Task<void>;
  auto send_put_batch(std::vector<Event> events, diagnostic_handler& dh)
    -> Task<void>;
  auto send_hlc_batch(std::vector<Event> events, diagnostic_handler& dh)
    -> Task<void>;

  ToCloudWatchArgs args_;
  ToMethod method_ = ToMethod::put_log_events;
  Option<Arc<Aws::CloudWatchLogs::CloudWatchLogsClient>> client_;
  Option<Box<HttpPool>> http_pool_;
  bool use_local_http_put_ = false;
  std::string token_;
  std::vector<Event> batch_;
  uint64_t batch_size_ = 1000;
  duration batch_timeout_ = std::chrono::seconds{1};
  Option<time> batch_started_at_;
  uint64_t parallel_ = 1;
  Semaphore request_slots_{1};
  MetricsCounter bytes_write_counter_;
};

} // namespace tenzir::plugins::cloudwatch
