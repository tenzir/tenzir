//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "operators_neo.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/variant.hpp>

#include <arrow/array/array_binary.h>
#include <aws/core/utils/Outcome.h>
#include <aws/sqs/model/Message.h>
#include <aws/sqs/model/MessageSystemAttributeName.h>

#include <charconv>

namespace tenzir::plugins::sqs {

namespace {

/// Resolves AWS credentials and creates an initialized `AsyncSqsQueue`.
template <class Args>
auto make_async_sqs_queue(const Args& args, OpCtx& ctx)
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
  auto poll_time = default_poll_time;
  if constexpr (requires { args.poll_time; }) {
    if (args.poll_time) {
      poll_time = std::chrono::duration_cast<std::chrono::seconds>(
        args.poll_time->inner);
    }
  }
  // Strip the sqs:// scheme prefix if present (e.g. `from_sqs "sqs://name"`).
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

} // namespace

// --- default_to_sqs_message_expression ---

auto default_to_sqs_message_expression() -> ast::expression {
  auto function
    = ast::entity{{ast::identifier{"print_ndjson", location::unknown}}};
  // Defaults bypass parser resolution in `OperatorPlugin`, so the entity
  // reference must be pre-resolved here.
  function.ref
    = entity_path{std::string{entity_pkg_std}, {"print_ndjson"}, entity_ns::fn};
  return ast::function_call{
    std::move(function),
    {ast::this_{location::unknown}},
    location::unknown,
    true,
  };
}

// --- FromSqs ---

FromSqs::FromSqs(FromSqsArgs args) : args_{std::move(args)} {
}

auto FromSqs::start(OpCtx& ctx) -> Task<void> {
  poll_time_ = args_.poll_time
                 ? std::chrono::duration_cast<std::chrono::seconds>(
                     args_.poll_time->inner)
                 : default_poll_time;
  queue_ = co_await make_async_sqs_queue(args_, ctx);
}

auto FromSqs::await_task(diagnostic_handler& dh) const -> Task<Any> {
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

namespace {

/// Returns the value of a system attribute, or nullptr if absent.
auto find_attribute(const Aws::SQS::Model::Message& message,
                    Aws::SQS::Model::MessageSystemAttributeName name)
  -> const Aws::String* {
  const auto& attrs = message.GetAttributes();
  auto it = attrs.find(name);
  if (it == attrs.end()) {
    return nullptr;
  }
  return &it->second;
}

/// Parses an SQS epoch-millisecond timestamp string into a `time`.
auto parse_epoch_ms(const Aws::String& value) -> std::optional<time> {
  auto ms = int64_t{};
  auto [ptr, ec]
    = std::from_chars(value.data(), value.data() + value.size(), ms);
  if (ec != std::errc{}) {
    return std::nullopt;
  }
  return time{std::chrono::milliseconds{ms}};
}

auto build_event(multi_series_builder& msb,
                 const Aws::SQS::Model::Message& message) -> void {
  using Attr = Aws::SQS::Model::MessageSystemAttributeName;
  auto event = msb.record();
  const auto& body = message.GetBody();
  event.field("message").data(
    std::string{std::string_view{body.data(), body.size()}});
  const auto& id = message.GetMessageId();
  event.field("message_id")
    .data(std::string{std::string_view{id.data(), id.size()}});
  if (const auto* value = find_attribute(message, Attr::SentTimestamp)) {
    if (auto t = parse_epoch_ms(*value)) {
      event.field("sent_time").data(*t);
    }
  }
  if (const auto* value
      = find_attribute(message, Attr::ApproximateFirstReceiveTimestamp)) {
    if (auto t = parse_epoch_ms(*value)) {
      event.field("first_receive_time").data(*t);
    }
  }
  if (const auto* value
      = find_attribute(message, Attr::ApproximateReceiveCount)) {
    auto n = int64_t{};
    auto [ptr, ec]
      = std::from_chars(value->data(), value->data() + value->size(), n);
    if (ec == std::errc{}) {
      event.field("receive_count").data(n);
    }
  }
  if (const auto* value = find_attribute(message, Attr::SenderId)) {
    event.field("sender_id")
      .data(std::string{std::string_view{value->data(), value->size()}});
  }
  if (const auto* value = find_attribute(message, Attr::MessageGroupId)) {
    event.field("message_group_id")
      .data(std::string{std::string_view{value->data(), value->size()}});
  }
  if (const auto* value
      = find_attribute(message, Attr::MessageDeduplicationId)) {
    event.field("message_deduplication_id")
      .data(std::string{std::string_view{value->data(), value->size()}});
  }
  if (const auto* value = find_attribute(message, Attr::SequenceNumber)) {
    event.field("sequence_number")
      .data(std::string{std::string_view{value->data(), value->size()}});
  }
}

} // namespace

auto FromSqs::process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> {
  auto messages = std::move(result).as<Aws::Vector<Aws::SQS::Model::Message>>();
  if (messages.empty()) {
    co_return;
  }
  auto opts = multi_series_builder::options{};
  opts.settings.ordered = true;
  opts.settings.raw = true;
  opts.settings.default_schema_name = "tenzir.sqs";
  auto msb = multi_series_builder{std::move(opts), ctx.dh()};
  for (const auto& message : messages) {
    build_event(msb, message);
  }
  for (auto&& slice : msb.finalize_as_table_slice()) {
    co_await push(std::move(slice));
  }
  for (const auto& message : messages) {
    auto diag = co_await queue_->delete_message(message);
    if (diag) {
      ctx.dh().emit(std::move(*diag));
    }
  }
}

// --- ToSqs ---

ToSqs::ToSqs(ToSqsArgs args) : args_{std::move(args)} {
}

auto ToSqs::start(OpCtx& ctx) -> Task<void> {
  queue_ = co_await make_async_sqs_queue(args_, ctx);
}

auto ToSqs::process(table_slice input, OpCtx& ctx) -> Task<void> {
  if (input.rows() == 0 or not queue_) {
    co_return;
  }
  auto& dh = ctx.dh();
  for (const auto& messages : eval(args_.message, input, dh)) {
    const auto impl = [&](const auto& array) -> Task<void> {
      for (auto i = int64_t{0}; i < array.length(); ++i) {
        if (array.IsNull(i)) {
          diagnostic::warning("expected `string` or `blob`, got `null`")
            .primary(args_.message)
            .emit(dh);
          continue;
        }
        auto bytes = as_bytes(array.Value(i));
        co_await queue_->send_message(Aws::String{
          reinterpret_cast<const char*>(bytes.data()), bytes.size()});
      }
    };
    if (auto strings = messages.template as<string_type>()) {
      co_await impl(*strings->array);
      continue;
    }
    if (auto blob = messages.template as<blob_type>()) {
      co_await impl(*blob->array);
      continue;
    }
    diagnostic::warning("expected `string` or `blob`, got `{}`",
                        messages.type.kind())
      .primary(args_.message)
      .note("event is skipped")
      .emit(dh);
  }
}

auto ToSqs::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  TENZIR_UNUSED(ctx);
  co_return FinalizeBehavior::done;
}

} // namespace tenzir::plugins::sqs
