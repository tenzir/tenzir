//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "operators_neo.hpp"

#include <tenzir/aws_iam.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/detail/env.hpp>

#include <aws/core/utils/Outcome.h>
#include <aws/sqs/model/Message.h>

namespace tenzir::plugins::sqs {

// --- make_async_sqs_queue ---

auto make_async_sqs_queue(const sqs_args& args, OpCtx& ctx)
  -> Task<std::shared_ptr<AsyncSqsQueue>> {
  auto aws_iam = args.aws_iam ? std::optional<located<record>>{*args.aws_iam}
                              : std::nullopt;
  auto aws_region = args.aws_region
                      ? std::optional<located<std::string>>{*args.aws_region}
                      : std::nullopt;
  auto auth = co_await resolve_aws_iam_auth(std::move(aws_iam),
                                            std::move(aws_region), ctx);
  if (not auth) {
    diagnostic::error("failed to initialize SQS queue")
      .primary(args.queue.source)
      .throw_();
  }
  auto resolved_creds = std::move(auth->credentials);
  // Resolve the effective region: explicit `aws_region` wins, then the region
  // from resolved AWS IAM credentials, then the AWS_REGION / AWS_DEFAULT_REGION
  // environment variables, and finally a default of "us-east-1".
  auto region = std::string{};
  if (args.aws_region) {
    region = args.aws_region->inner;
  } else if (resolved_creds and not resolved_creds->region.empty()) {
    region = resolved_creds->region;
  } else if (auto env = detail::getenv("AWS_REGION")) {
    region = *env;
  } else if (auto env = detail::getenv("AWS_DEFAULT_REGION")) {
    region = *env;
  } else {
    region = "us-east-1";
  }
  auto poll_time = args.poll_time
                     ? std::chrono::duration_cast<std::chrono::seconds>(
                         args.poll_time->inner)
                     : default_poll_time;
  // Strip the sqs:// scheme prefix if present (e.g. `load_sqs "sqs://name"`).
  auto queue_name = args.queue;
  if (queue_name.inner.starts_with("sqs://")) {
    queue_name.inner.erase(0, 6);
  }
  auto queue
    = std::make_shared<AsyncSqsQueue>(std::move(queue_name), poll_time,
                                      std::move(region), resolved_creds,
                                      ctx.io_executor());
  co_await queue->init();
  co_return queue;
}

// --- LoadSqs ---

LoadSqs::LoadSqs(sqs_args args) : args_{std::move(args)} {
}

auto LoadSqs::start(OpCtx& ctx) -> Task<void> {
  poll_time_ = args_.poll_time
                 ? std::chrono::duration_cast<std::chrono::seconds>(
                     args_.poll_time->inner)
                 : default_poll_time;
  queue_ = co_await make_async_sqs_queue(args_, ctx);
}

auto LoadSqs::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  constexpr auto num_messages = size_t{1};
  auto error = std::exception_ptr{};
  try {
    auto messages = co_await queue_->receive_messages(num_messages, poll_time_);
    co_return std::move(messages);
  } catch (...) {
    error = std::current_exception();
  }
  // The HTTP pool does not support folly cancellation tokens, so a
  // shutdown-triggered connection reset surfaces as an HTTP error rather
  // than OperationCancelled.  Convert it to a clean cancellation when the
  // operator scope has already been cancelled.
  co_await folly::coro::co_safe_point;
  std::rethrow_exception(error);
}

auto LoadSqs::process_task(Any result, Push<chunk_ptr>& push, OpCtx& ctx)
  -> Task<void> {
  auto messages = std::move(result).as<Aws::Vector<Aws::SQS::Model::Message>>();
  for (const auto& message : messages) {
    const auto& body = message.GetBody();
    auto str = std::string_view{body.data(), body.size()};
    co_await push(chunk::copy(str));
    auto diag = co_await queue_->delete_message(message);
    if (diag) {
      ctx.dh().emit(std::move(*diag));
    }
  }
}

// --- SaveSqs ---

SaveSqs::SaveSqs(sqs_args args) : args_{std::move(args)} {
}

auto SaveSqs::start(OpCtx& ctx) -> Task<void> {
  // poll_time is deprecated for save_sqs; always use the default.
  args_.poll_time = std::nullopt;
  queue_ = co_await make_async_sqs_queue(args_, ctx);
}

auto SaveSqs::process(chunk_ptr input, OpCtx& ctx) -> Task<void> {
  TENZIR_UNUSED(ctx);
  if (not input or input->size() == 0) {
    co_return;
  }
  co_await queue_->send_message(to_aws_string(std::move(input)));
}

auto SaveSqs::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  TENZIR_UNUSED(ctx);
  co_return FinalizeBehavior::done;
}

} // namespace tenzir::plugins::sqs
