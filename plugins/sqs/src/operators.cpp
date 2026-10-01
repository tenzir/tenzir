//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "operators.hpp"

#include <tenzir/amazon.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval_kernel.hpp>
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
  auto aws_iam = args.aws_iam ? Option<located<record>>{*args.aws_iam} : None{};
  auto aws_region
    = args.aws_region ? Option<located<std::string>>{*args.aws_region} : None{};
  auto auth = co_await resolve_aws_iam_auth(std::move(aws_iam),
                                            std::move(aws_region), ctx);
  if (not auth) {
    diagnostic::error("failed to initialize SQS queue")
      .primary(args.queue.source)
      .throw_();
  }
  auto resolved_creds = std::move(auth->credentials);
  // Strip the sqs:// scheme prefix if present (e.g.
  // `from_amazon_sqs "sqs://name"`). The argument may also be a full queue URL
  // (`https://...`), in which case we leave it intact and let `AsyncSqsQueue`
  // skip URL resolution.
  auto queue_name = args.queue;
  if (queue_name.inner.starts_with("sqs://")) {
    queue_name.inner.erase(0, 6);
  }
  // Reject anything that isn't either a valid AWS SQS queue name or an HTTP(S)
  // queue URL — this catches malformed inputs like `sqs://https://...` that
  // would otherwise silently break region detection and SigV4 signing.
  if (not is_sqs_queue_url(queue_name.inner)
      and not is_valid_sqs_queue_name(queue_name.inner)) {
    diagnostic::error("invalid SQS queue `{}`", args.queue.inner)
      .primary(args.queue.source)
      .hint("expected a queue name, `sqs://<name>`, or a full queue URL")
      .throw_();
  }
  // Resolve the effective region in priority order:
  //   1. Explicit `aws_region` (user override).
  //   2. Region parsed from a full queue URL (e.g. the `us-west-2` in
  //      `https://sqs.us-west-2.amazonaws.com/...`) — keeps SigV4 signing
  //      aligned with the queue's region without forcing the user to set
  //      `aws_region` redundantly.
  //   3. Region from resolved AWS IAM credentials.
  //   4. Environment variables or the AWS SDK default.
  auto region_hint = args.aws_region
                       ? Option<std::string>{args.aws_region->inner}
                       : region_from_sqs_url(queue_name.inner);
  auto region = amazon::resolve_region(std::move(region_hint), resolved_creds);
  auto poll_time = default_poll_time;
  if constexpr (requires { args.poll_time; }) {
    if (args.poll_time) {
      poll_time = std::chrono::duration_cast<std::chrono::seconds>(
        args.poll_time->inner);
    }
  }
  auto creds_opt
    = resolved_creds
        ? Option<tenzir::resolved_aws_credentials>{std::move(*resolved_creds)}
        : None{};
  auto queue
    = std::make_shared<AsyncSqsQueue>(std::move(queue_name), poll_time,
                                      std::move(region), std::move(creds_opt),
                                      ctx.io_executor());
  co_await queue->init();
  co_return queue;
}

} // namespace

// --- default_to_amazon_sqs_message_expression ---

auto default_to_amazon_sqs_message_expression() -> ast::expression {
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

// --- SqsReceiver ---

auto SqsReceiver::start(FromSqsArgs const& args, OpCtx& ctx) -> Task<void> {
  batch_size_ = args.batch_size
                  ? detail::narrow_cast<size_t>(args.batch_size->inner)
                  : size_t{1};
  poll_time_ = args.poll_time
                 ? std::chrono::duration_cast<std::chrono::seconds>(
                     args.poll_time->inner)
                 : default_poll_time;
  visibility_timeout_
    = args.visibility_timeout
        ? Option{std::chrono::duration_cast<std::chrono::seconds>(
            args.visibility_timeout->inner)}
        : None{};
  keep_messages_ = args.keep_messages;
  queue_ = co_await make_async_sqs_queue(args, ctx);
  bytes_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_sqs"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_sqs"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::events);
}

auto SqsReceiver::receive() const -> Task<Any> {
  auto error = std::exception_ptr{};
  try {
    auto messages = co_await queue_->receive_messages(batch_size_, poll_time_,
                                                      visibility_timeout_);
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

auto SqsReceiver::count_bytes(
  Aws::Vector<Aws::SQS::Model::Message> const& messages) -> void {
  for (const auto& message : messages) {
    const auto& body = message.GetBody();
    if (not body.empty()) {
      bytes_read_counter_.add(body.size());
    }
  }
}

auto SqsReceiver::count_events(uint64_t count) -> void {
  events_read_counter_.add(count);
}

auto SqsReceiver::acknowledge(
  Aws::Vector<Aws::SQS::Model::Message> const& messages, OpCtx& ctx)
  -> Task<void> {
  if (keep_messages_) {
    co_return;
  }
  for (const auto& message : messages) {
    auto diag = co_await queue_->delete_message(message);
    if (diag) {
      ctx.dh().emit(std::move(*diag));
    }
  }
}

// --- SqsSender ---

auto SqsSender::start(ToSqsArgs const& args, OpCtx& ctx) -> Task<void> {
  queue_ = co_await make_async_sqs_queue(args, ctx);
  bytes_write_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_amazon_sqs"},
                       MetricsDirection::write, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_write_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_amazon_sqs"},
                       MetricsDirection::write, MetricsVisibility::external_,
                       MetricsUnit::events);
}

auto SqsSender::ready() const -> bool {
  return queue_ != nullptr;
}

auto SqsSender::send(std::span<std::byte const> bytes) -> Task<void> {
  co_await queue_->send_message(
    Aws::String{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
  if (not bytes.empty()) {
    bytes_write_counter_.add(bytes.size());
  }
  events_write_counter_.add(1);
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
auto parse_epoch_ms(const Aws::String& value) -> Option<time> {
  auto ms = int64_t{};
  auto [ptr, ec]
    = std::from_chars(value.data(), value.data() + value.size(), ms);
  if (ec != std::errc{}) {
    return None{};
  }
  return time{std::chrono::milliseconds{ms}};
}

auto to_string_view(const Aws::String& value) -> std::string_view {
  return std::string_view{value.data(), value.size()};
}

/// Writes the fields of `message` into `event`, a record of either a
/// `multi_series_builder` or a `nova::ArrayBuilder<nova::Record>`.
auto build_event(auto event, const Aws::SQS::Model::Message& message) -> void {
  using Attr = Aws::SQS::Model::MessageSystemAttributeName;
  event.field("message").data(std::string{to_string_view(message.GetBody())});
  event.field("message_id")
    .data(std::string{to_string_view(message.GetMessageId())});
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
    event.field("sender_id").data(std::string{to_string_view(*value)});
  }
  if (const auto* value = find_attribute(message, Attr::MessageGroupId)) {
    event.field("message_group_id").data(std::string{to_string_view(*value)});
  }
  if (const auto* value
      = find_attribute(message, Attr::MessageDeduplicationId)) {
    event.field("message_deduplication_id")
      .data(std::string{to_string_view(*value)});
  }
  if (const auto* value = find_attribute(message, Attr::SequenceNumber)) {
    event.field("sequence_number").data(std::string{to_string_view(*value)});
  }
}

} // namespace

// --- FromSqs ---

FromSqs::FromSqs(FromSqsArgs args) : args_{std::move(args)} {
}

auto FromSqs::start(OpCtx& ctx) -> Task<void> {
  co_await receiver_.start(args_, ctx);
}

auto FromSqs::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  return receiver_.receive();
}

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
  receiver_.count_bytes(messages);
  for (const auto& message : messages) {
    build_event(msb.record(), message);
  }
  for (auto&& slice : msb.finalize_as_table_slice()) {
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    receiver_.count_events(rows);
  }
  co_await receiver_.acknowledge(messages, ctx);
}

// --- FromSqsEvents ---

FromSqsEvents::FromSqsEvents(FromSqsArgs args) : args_{std::move(args)} {
}

auto FromSqsEvents::start(OpCtx& ctx) -> Task<void> {
  co_await receiver_.start(args_, ctx);
}

auto FromSqsEvents::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  return receiver_.receive();
}

auto FromSqsEvents::process_task(Any result, Push<nova::Events>& push,
                                 OpCtx& ctx) -> Task<void> {
  auto messages = std::move(result).as<Aws::Vector<Aws::SQS::Model::Message>>();
  if (messages.empty()) {
    co_return;
  }
  receiver_.count_bytes(messages);
  auto builder = nova::ArrayBuilder<nova::Record>{};
  for (const auto& message : messages) {
    build_event(builder.record(), message);
  }
  auto data = builder.finish();
  auto const rows = data.length();
  co_await push(
    nova::Events{std::move(data), nova::storage::BitMap{rows, true},
                 nova::Events::Meta::make_empty(rows, "tenzir.sqs")});
  receiver_.count_events(detail::narrow<uint64_t>(rows));
  co_await receiver_.acknowledge(messages, ctx);
}

// --- ToSqs ---

ToSqs::ToSqs(ToSqsArgs args) : args_{std::move(args)} {
}

auto ToSqs::start(OpCtx& ctx) -> Task<void> {
  co_await sender_.start(args_, ctx);
}

auto ToSqs::process(table_slice input, OpCtx& ctx) -> Task<void> {
  if (input.rows() == 0 or not sender_.ready()) {
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
        co_await sender_.send(as_bytes(array.Value(i)));
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

// --- ToSqsEvents ---

ToSqsEvents::ToSqsEvents(ToSqsArgs args) : args_{std::move(args)} {
}

auto ToSqsEvents::start(OpCtx& ctx) -> Task<void> {
  co_await sender_.start(args_, ctx);
  auto message = co_await nova::Evaluator::make(args_.message, ctx);
  if (not message) {
    co_return;
  }
  message_.emplace(std::move(*message));
}

auto ToSqsEvents::process(nova::Events input, OpCtx& ctx) -> Task<void> {
  if (not input.mask.any() or not sender_.ready()) {
    co_return;
  }
  if (not message_) {
    co_return;
  }
  auto& dh = ctx.dh();
  auto messages = message_->eval(input, nova::EvalCtx{dh});
  auto warn_null = nova::WarnOnce{};
  auto warn_type = nova::WarnOnce{};
  for (auto row : nova::storage::true_bits(input.mask)) {
    auto value = messages.get(row);
    if (auto text = try_as<nova::RowView<nova::String>>(value)) {
      co_await sender_.send(as_bytes(**text));
      continue;
    }
    if (auto bytes = try_as<nova::RowView<nova::Blob>>(value)) {
      co_await sender_.send(**bytes);
      continue;
    }
    if (is<nova::RowView<nova::Null>>(value)) {
      warn_null(dh, diagnostic::warning("expected `string` or `blob`, got "
                                        "`null`")
                      .primary(args_.message));
      continue;
    }
    match(value, [&]<class T>(nova::RowView<T> const&) {
      warn_type(dh, diagnostic::warning("expected `string` or `blob`, got `{}`",
                                        nova::Type<T>::static_name)
                      .primary(args_.message)
                      .note("event is skipped"));
    });
  }
}

auto ToSqsEvents::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  TENZIR_UNUSED(ctx);
  co_return FinalizeBehavior::done;
}

} // namespace tenzir::plugins::sqs
