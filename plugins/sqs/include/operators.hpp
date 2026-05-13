//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/tql2/ast.hpp>

#include <cstddef>
#include <memory>

#include "async_sqs_queue.hpp"
#include "operators_legacy.hpp"

namespace tenzir::plugins::sqs {

struct FromSqsArgs {
  located<std::string> queue;
  bool delete_messages = true;
  Option<located<uint64_t>> batch_size;
  Option<located<duration>> poll_time;
  Option<located<duration>> visibility_timeout;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  location operator_location;
};

struct ToSqsArgs {
  located<std::string> queue;
  ast::expression message;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  location operator_location;
};

/// Returns the default `message=` expression used by `to_sqs`
/// (`print_ndjson(this)`).
auto default_to_sqs_message_expression() -> ast::expression;

class FromSqs final : public Operator<void, table_slice> {
public:
  explicit FromSqs(FromSqsArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;

private:
  FromSqsArgs args_;
  size_t batch_size_ = 1;
  std::chrono::seconds poll_time_ = default_poll_time;
  Option<std::chrono::seconds> visibility_timeout_;
  std::shared_ptr<AsyncSqsQueue> queue_;
  MetricsCounter bytes_read_counter_;
};

class ToSqs final : public Operator<table_slice, void> {
public:
  explicit ToSqs(ToSqsArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(table_slice input, OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;

private:
  ToSqsArgs args_;
  std::shared_ptr<AsyncSqsQueue> queue_;
  MetricsCounter bytes_write_counter_;
};

} // namespace tenzir::plugins::sqs
