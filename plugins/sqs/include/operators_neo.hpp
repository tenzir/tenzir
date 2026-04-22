//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/fwd.hpp>

#include <memory>

#include "async_sqs_queue.hpp"
#include "operators_legacy.hpp"

namespace tenzir::plugins::sqs {

struct sqs_args {
  located<std::string> queue;
  Option<located<duration>> poll_time;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  location operator_location;
};

/// Resolves AWS credentials and creates an initialized AsyncSqsQueue.
auto make_async_sqs_queue(const sqs_args& args, OpCtx& ctx)
  -> Task<std::shared_ptr<AsyncSqsQueue>>;

class LoadSqs final : public Operator<void, chunk_ptr> {
public:
  explicit LoadSqs(sqs_args args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override;

private:
  sqs_args args_;
  std::chrono::seconds poll_time_ = default_poll_time;
  std::shared_ptr<AsyncSqsQueue> queue_;
};

class SaveSqs final : public Operator<chunk_ptr, void> {
public:
  explicit SaveSqs(sqs_args args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(chunk_ptr input, OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;

private:
  sqs_args args_;
  std::shared_ptr<AsyncSqsQueue> queue_;
};

} // namespace tenzir::plugins::sqs
