//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir_plugins/nats/common.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/array/builder_binary.h>
#include <arrow/record_batch.h>
#include <arrow/util/utf8.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/fibers/Semaphore.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace tenzir::plugins::nats {

namespace {

using namespace tenzir::si_literals;

constexpr auto default_batch_size = uint64_t{1_Ki / 8};
constexpr auto default_queue_capacity = uint64_t{1_Ki};
constexpr auto max_queue_capacity
  = uint64_t{std::numeric_limits<uint32_t>::max()} - 2;
constexpr auto shutdown_flush_timeout_ms = int64_t{5_k};
constexpr auto direct_read_lines_schema = std::string_view{"tenzir.line"};
constexpr auto direct_metadata_field = std::string_view{"__tenzir_nats"};

struct FromNatsArgs {
  located<std::string> subject;
  Option<located<secret>> url;
  Option<located<std::string>> durable;
  Option<located<uint64_t>> count;
  Option<located<data>> tls;
  Option<located<data>> auth;
  uint64_t batch_size = default_batch_size;
  uint64_t queue_capacity = default_queue_capacity;
  located<ir::pipeline> parser;
  location op;
  let_id subject_var;
  let_id reply_var;
  let_id headers_var;
  let_id stream_var;
  let_id consumer_var;
  let_id stream_sequence_var;
  let_id consumer_sequence_var;
  let_id num_delivered_var;
  let_id num_pending_var;
  let_id timestamp_var;
};

struct IncomingMessage {
  nats_msg_ptr msg;
};

struct SubscriptionComplete {
  natsStatus status;
};

struct FlushDirect {};

using SourceMessage
  = variant<IncomingMessage, SubscriptionComplete, FlushDirect>;
using SourceBatch = std::vector<SourceMessage>;
using SourceQueue = folly::coro::BoundedQueue<SourceMessage>;

enum class DirectParser {
  none,
  read_lines,
};

enum class DirectReadLinesSetup {
  enabled,
  fallback,
  error,
};

struct MetadataUsage {
  bool subject = false;
  bool reply = false;
  bool headers = false;
  bool stream = false;
  bool consumer = false;
  bool stream_sequence = false;
  bool consumer_sequence = false;
  bool num_delivered = false;
  bool num_pending = false;
  bool timestamp = false;

  auto any() const -> bool {
    return subject or reply or headers or stream or consumer or stream_sequence
           or consumer_sequence or num_delivered or num_pending or timestamp;
  }

  auto jetstream() const -> bool {
    return stream or consumer or stream_sequence or consumer_sequence
           or num_delivered or num_pending or timestamp;
  }
};

struct js_stream_names_list_deleter {
  auto operator()(jsStreamNamesList* ptr) const noexcept -> void {
    if (ptr) {
      jsStreamNamesList_Destroy(ptr);
    }
  }
};

struct js_consumer_info_deleter {
  auto operator()(jsConsumerInfo* ptr) const noexcept -> void {
    if (ptr) {
      jsConsumerInfo_Destroy(ptr);
    }
  }
};

using js_stream_names_list_ptr
  = std::unique_ptr<jsStreamNamesList, js_stream_names_list_deleter>;
using js_consumer_info_ptr
  = std::unique_ptr<jsConsumerInfo, js_consumer_info_deleter>;

struct SourceState {
  explicit SourceState(uint64_t capacity)
    : queue{detail::narrow_cast<uint32_t>(capacity + 2)},
      message_slots{capacity} {
  }

  // The queue has `message_slots` plus one slot for the single delayed direct
  // flush and one slot for the first terminal completion.
  SourceQueue queue;
  folly::fibers::Semaphore message_slots;
  Atomic<bool> stopping = false;
  Atomic<bool> terminal_completion_queued = false;
};

auto normal_completion(natsStatus status) -> bool {
  return status == NATS_MAX_DELIVERED_MSGS or status == NATS_TIMEOUT
         or status == NATS_NOT_FOUND or status == NATS_LIMIT_REACHED;
}

void message_callback(natsConnection*, natsSubscription*, natsMsg* msg,
                      void* closure) {
  auto* state = static_cast<SourceState*>(closure);
  TENZIR_ASSERT(state);
  if (state->stopping.load(std::memory_order_acquire)) {
    natsMsg_Nak(msg, nullptr);
    natsMsg_Destroy(msg);
    return;
  }
  if (not state->message_slots.try_wait()) {
    natsMsg_Nak(msg, nullptr);
    natsMsg_Destroy(msg);
    return;
  }
  if (state->queue.try_enqueue(
        SourceMessage{IncomingMessage{nats_msg_ptr{msg}}})) {
    return;
  }
  state->message_slots.signal();
  natsMsg_Nak(msg, nullptr);
  natsMsg_Destroy(msg);
}

void complete_callback(natsConnection*, natsSubscription*, natsStatus status,
                       void* closure) {
  auto* state = static_cast<SourceState*>(closure);
  TENZIR_ASSERT(state);
  if (state->stopping.load(std::memory_order_acquire)) {
    return;
  }
  // Normal fetch completions are non-terminal. The queue has one slot beyond
  // the data-message capacity, reserved for the first terminal completion.
  if (status == NATS_OK or normal_completion(status)) {
    return;
  }
  if (state->terminal_completion_queued.exchange(true,
                                                 std::memory_order_acq_rel)) {
    return;
  }
  [[maybe_unused]] auto enqueued
    = state->queue.try_enqueue(SourceMessage{SubscriptionComplete{status}});
  TENZIR_ASSERT(enqueued);
}

auto get_optional_c_string(char const* str) -> ast::constant::kind {
  if (str and *str) {
    return std::string{str};
  }
  return caf::none;
}

auto message_headers(natsMsg* msg) -> record {
  auto result = record{};
  auto const** keys = static_cast<char const**>(nullptr);
  auto key_count = int{};
  auto status = natsMsgHeader_Keys(msg, &keys, &key_count);
  if (status == NATS_NOT_FOUND) {
    return result;
  }
  if (status != NATS_OK) {
    return result;
  }
  auto keys_guard = std::unique_ptr<char const*, decltype(&std::free)>{
    const_cast<char const**>(keys),
    std::free,
  };
  for (auto i = 0; i < key_count; ++i) {
    auto const* key = keys[i];
    if (key == nullptr) {
      continue;
    }
    auto const** values = static_cast<char const**>(nullptr);
    auto value_count = int{};
    status = natsMsgHeader_Values(msg, key, &values, &value_count);
    if (status != NATS_OK) {
      continue;
    }
    auto values_guard = std::unique_ptr<char const*, decltype(&std::free)>{
      const_cast<char const**>(values),
      std::free,
    };
    auto xs = list{};
    xs.reserve(static_cast<size_t>(value_count));
    for (auto j = 0; j < value_count; ++j) {
      xs.emplace_back(values[j] ? std::string{values[j]} : std::string{});
    }
    result[key] = std::move(xs);
  }
  return result;
}

auto message_metadata(natsMsg* msg) -> js_msg_meta_data_ptr {
  auto* raw = static_cast<jsMsgMetaData*>(nullptr);
  if (natsMsg_GetMetaData(&raw, msg) != NATS_OK) {
    return {};
  }
  return js_msg_meta_data_ptr{raw};
}

auto make_payload_chunk(natsMsg* msg) -> chunk_ptr {
  auto const size = natsMsg_GetDataLength(msg);
  if (size <= 0) {
    return chunk::make_empty();
  }
  return chunk::copy(natsMsg_GetData(msg), static_cast<size_t>(size));
}

auto add_metadata_to_env(FromNatsArgs const& args, MetadataUsage const& usage,
                         natsMsg* msg, substitute_ctx::env_t& env) -> void {
  if (usage.subject) {
    env[args.subject_var] = get_optional_c_string(natsMsg_GetSubject(msg));
  }
  if (usage.reply) {
    env[args.reply_var] = get_optional_c_string(natsMsg_GetReply(msg));
  }
  if (usage.headers) {
    env[args.headers_var] = message_headers(msg);
  }
  auto meta
    = usage.jetstream() ? message_metadata(msg) : js_msg_meta_data_ptr{};
  if (usage.stream) {
    env[args.stream_var]
      = meta ? get_optional_c_string(meta->Stream) : caf::none;
  }
  if (usage.consumer) {
    env[args.consumer_var]
      = meta ? get_optional_c_string(meta->Consumer) : caf::none;
  }
  if (usage.stream_sequence) {
    if (meta) {
      env[args.stream_sequence_var] = meta->Sequence.Stream;
    } else {
      env[args.stream_sequence_var] = caf::none;
    }
  }
  if (usage.consumer_sequence) {
    if (meta) {
      env[args.consumer_sequence_var] = meta->Sequence.Consumer;
    } else {
      env[args.consumer_sequence_var] = caf::none;
    }
  }
  if (usage.num_delivered) {
    if (meta) {
      env[args.num_delivered_var] = meta->NumDelivered;
    } else {
      env[args.num_delivered_var] = caf::none;
    }
  }
  if (usage.num_pending) {
    if (meta) {
      env[args.num_pending_var] = meta->NumPending;
    } else {
      env[args.num_pending_var] = caf::none;
    }
  }
  if (usage.timestamp) {
    if (meta) {
      env[args.timestamp_var] = time{} + duration{meta->Timestamp};
    } else {
      env[args.timestamp_var] = caf::none;
    }
  }
}

auto metadata_usage(FromNatsArgs const& args, ir::pipeline const& pipe)
  -> MetadataUsage {
  return {
    .subject = pipe.references(args.subject_var),
    .reply = pipe.references(args.reply_var),
    .headers = pipe.references(args.headers_var),
    .stream = pipe.references(args.stream_var),
    .consumer = pipe.references(args.consumer_var),
    .stream_sequence = pipe.references(args.stream_sequence_var),
    .consumer_sequence = pipe.references(args.consumer_sequence_var),
    .num_delivered = pipe.references(args.num_delivered_var),
    .num_pending = pipe.references(args.num_pending_var),
    .timestamp = pipe.references(args.timestamp_var),
  };
}

auto direct_metadata_expr(std::string_view field, location loc)
  -> ast::expression {
  auto root = ast::expression{ast::root_field{
    ast::identifier{std::string{direct_metadata_field}, loc},
  }};
  return ast::field_access{
    std::move(root),
    loc,
    false,
    ast::identifier{std::string{field}, loc},
  };
}

auto direct_metadata_replacements(FromNatsArgs const& args)
  -> std::vector<ast::dollar_var_replacement> {
  auto const loc = args.parser.source;
  return {
    {args.subject_var, direct_metadata_expr("subject", loc)},
    {args.reply_var, direct_metadata_expr("reply", loc)},
    {args.headers_var, direct_metadata_expr("headers", loc)},
    {args.stream_var, direct_metadata_expr("stream", loc)},
    {args.consumer_var, direct_metadata_expr("consumer", loc)},
    {args.stream_sequence_var, direct_metadata_expr("stream_sequence", loc)},
    {args.consumer_sequence_var,
     direct_metadata_expr("consumer_sequence", loc)},
    {args.num_delivered_var, direct_metadata_expr("num_delivered", loc)},
    {args.num_pending_var, direct_metadata_expr("num_pending", loc)},
    {args.timestamp_var, direct_metadata_expr("timestamp", loc)},
  };
}

auto direct_metadata_drop_field(location loc) -> ast::field_path {
  return ast::field_path::try_from(ast::root_field{
                                     ast::identifier{
                                       std::string{direct_metadata_field},
                                       loc,
                                     },
                                   })
    .value();
}

auto direct_read_lines_type() -> type {
  return type{
    std::string{direct_read_lines_schema},
    record_type{{"line", string_type{}}},
  };
}

auto direct_parser_for(FromNatsArgs const& args) -> DirectParser {
  auto const& pipe = args.parser.inner;
  if (pipe.operators.empty()) {
    return DirectParser::none;
  }
  if (pipe.operators.front()->is_default_invocation("read_lines")) {
    return DirectParser::read_lines;
  }
  return DirectParser::none;
}

auto schedule_direct_flush(Arc<SourceState> source,
                           series_builder::duration wait_for) -> Task<void> {
  co_await sleep_for(wait_for);
  if (source->stopping.load(std::memory_order_acquire)) {
    co_return;
  }
  co_await source->queue.enqueue(SourceMessage{FlushDirect{}});
}

class FromNats final : public Operator<void, table_slice> {
public:
  explicit FromNats(FromNatsArgs args)
    : args_{std::move(args)}, source_{std::in_place, args_.queue_capacity} {
    parser_metadata_usage_ = metadata_usage(args_, args_.parser.inner);
    direct_parser_ = direct_parser_for(args_);
  }

  FromNats(FromNats const&) = delete;
  auto operator=(FromNats const&) -> FromNats& = delete;
  FromNats(FromNats&&) noexcept = default;
  auto operator=(FromNats&&) noexcept -> FromNats& = default;

  ~FromNats() override {
    request_stop();
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    read_bytes_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_nats"},
                         MetricsDirection::read, MetricsVisibility::external_);
    if (direct_parser_ == DirectParser::read_lines) {
      auto setup = co_await prepare_direct_read_lines(ctx);
      if (setup == DirectReadLinesSetup::error) {
        done_ = true;
        co_return;
      }
      if (setup == DirectReadLinesSetup::fallback) {
        direct_parser_ = DirectParser::none;
      }
    }
    if (direct_parser_ == DirectParser::none) {
      auto ok = prepare_generic_parser(ctx);
      if (not ok) {
        done_ = true;
        co_return;
      }
    }
    auto resolved
      = co_await resolve_connection_config(ctx, args_.url, args_.auth);
    if (not resolved) {
      done_ = true;
      co_return;
    }
    auto* evb = ctx.io_executor()->getEventBase();
    TENZIR_ASSERT(evb);
    auto options
      = make_nats_options(*resolved, args_.tls,
                          args_.url ? args_.url->source : location::unknown,
                          ctx.dh(), *evb);
    if (not options) {
      done_ = true;
      co_return;
    }
    options_ = std::move(*options);
    auto* raw_connection = static_cast<natsConnection*>(nullptr);
    auto status = co_await spawn_blocking([&] {
      return natsConnection_Connect(&raw_connection, options_.get());
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to connect to NATS server")
                        .primary(args_.url ? args_.url->source
                                           : location::unknown),
                      status, ctx.dh());
      done_ = true;
      co_return;
    }
    connection_ = nats_connection_ptr{raw_connection};
    auto* raw_js = static_cast<jsCtx*>(nullptr);
    status = natsConnection_JetStream(&raw_js, connection_.get(), nullptr);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to create JetStream context")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      co_return;
    }
    js_ = js_ctx_ptr{raw_js};
    auto durable_stream = Option<std::string>{};
    if (args_.durable) {
      durable_stream = co_await ensure_durable_consumer(ctx);
      if (not durable_stream) {
        done_ = true;
        co_return;
      }
    }
    auto js_options = jsOptions{};
    jsOptions_Init(&js_options);
    js_options.PullSubscribeAsync.FetchSize
      = detail::narrow_cast<int>(args_.batch_size);
    js_options.PullSubscribeAsync.KeepAhead
      = detail::narrow_cast<int>(std::max<uint64_t>(1, args_.batch_size / 2));
    js_options.PullSubscribeAsync.CompleteHandler = complete_callback;
    js_options.PullSubscribeAsync.CompleteHandlerClosure = &*source_;
    if (args_.count) {
      js_options.PullSubscribeAsync.MaxMessages
        = detail::narrow_cast<int>(args_.count->inner);
    }
    auto sub_options = jsSubOptions{};
    jsSubOptions_Init(&sub_options);
    sub_options.ManualAck = true;
    sub_options.Config.AckPolicy = desired_ack_policy();
    auto const* durable
      = args_.durable ? args_.durable->inner.c_str() : nullptr;
    if (not durable) {
      direct_ack_all_ = desired_ack_policy() == js_AckAll;
      sub_options.Config.MaxAckPending
        = detail::narrow_cast<int>(args_.queue_capacity);
    }
    if (durable) {
      TENZIR_ASSERT(durable_stream);
      sub_options.Stream = durable_stream->c_str();
      sub_options.Consumer = durable;
    }
    auto js_error = jsErrCode{};
    auto* raw_subscription = static_cast<natsSubscription*>(nullptr);
    status = co_await spawn_blocking([&] {
      return js_PullSubscribeAsync(&raw_subscription, js_.get(),
                                   args_.subject.inner.c_str(), durable,
                                   message_callback, &*source_, &js_options,
                                   &sub_options, &js_error);
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to subscribe to NATS subject")
                        .primary(args_.subject.source)
                        .note("JetStream error code: {}",
                              static_cast<int>(js_error)),
                      status, ctx.dh());
      done_ = true;
      co_return;
    }
    subscription_ = nats_subscription_ptr{raw_subscription};
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
    }
    auto batch = SourceBatch{};
    auto const max_batch_size = detail::narrow_cast<size_t>(
      std::min(args_.batch_size, args_.queue_capacity));
    batch.reserve(max_batch_size);
    batch.push_back(co_await source_->queue.dequeue());
    while (batch.size() < max_batch_size) {
      auto next = source_->queue.try_dequeue();
      if (not next) {
        break;
      }
      batch.push_back(std::move(*next));
    }
    co_return batch;
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto* batch = result.try_as<SourceBatch>();
    if (not batch) {
      co_return;
    }
    for (auto& message : *batch) {
      match(
        message,
        [&](IncomingMessage const&) {
          source_->message_slots.signal();
        },
        [](SubscriptionComplete const&) {},
        [](FlushDirect const&) {});
    }
    for (auto& message : *batch) {
      co_await process_source_message(std::move(message), push, ctx);
    }
  }

private:
  auto process_source_message(SourceMessage message, Push<table_slice>& push,
                              OpCtx& ctx) -> Task<void> {
    co_await co_match(
      std::move(message),
      [&](IncomingMessage item) -> Task<void> {
        if (done_) {
          natsMsg_Nak(item.msg.get(), nullptr);
          co_return;
        }
        if (direct_parser_ == DirectParser::read_lines) {
          co_await process_read_lines_message(std::move(item.msg), push, ctx);
        } else {
          co_await process_message(std::move(item.msg), ctx);
        }
        maybe_finish();
      },
      [&](SubscriptionComplete complete) -> Task<void> {
        if (not normal_completion(complete.status)
            and complete.status != NATS_OK) {
          emit_nats_error(diagnostic::error("NATS subscription ended with "
                                            "error")
                            .primary(args_.subject.source),
                          complete.status, ctx.dh());
          subscription_failed_ = true;
          maybe_finish();
        }
        co_return;
      },
      [&](FlushDirect) -> Task<void> {
        if (direct_parser_ == DirectParser::read_lines) {
          direct_flush_scheduled_ = false;
          co_await flush_direct_read_lines(push, ctx);
          maybe_finish();
        }
        co_return;
      });
  }

public:
  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    if (direct_tail_active_ and is<caf::none_t>(key)) {
      if (not direct_metadata_drop_fields_.empty()) {
        slice = drop(slice, direct_metadata_drop_fields_, ctx.dh(), false);
      }
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (direct_tail_active_ and is<caf::none_t>(key)) {
      direct_tail_finished_ = true;
      if (direct_tail_closing_) {
        done_ = true;
        request_stop();
      }
      co_return;
    }
    auto msg_id = detail::narrow_cast<uint64_t>(as<int64_t>(key));
    auto it = pending_.find(msg_id);
    if (it == pending_.end()) {
      co_return;
    }
    auto const acked = acknowledge(it->second.get(), ctx);
    pending_.erase(it);
    if (args_.count and received_ >= args_.count->inner and pending_.empty()) {
      if (acked) {
        co_await flush_acknowledgements(ctx);
      }
      done_ = true;
      request_stop();
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  enum class PendingMessages {
    nak,
    keep,
  };

  auto desired_ack_policy() const -> jsAckPolicy {
    return direct_parser_ == DirectParser::read_lines ? js_AckAll
                                                      : js_AckExplicit;
  }

  auto prepare_generic_parser(OpCtx& ctx) -> bool {
    if (parser_metadata_usage_.any()) {
      return true;
    }
    auto parser = args_.parser.inner;
    if (not parser.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      return false;
    }
    parser_template_ = std::move(parser);
    return true;
  }

  auto prepare_direct_read_lines(OpCtx& ctx) -> Task<DirectReadLinesSetup> {
    auto tail = args_.parser.inner;
    TENZIR_ASSERT(not tail.operators.empty());
    TENZIR_ASSERT(tail.operators.front()->is_default_invocation("read_lines"));
    tail.operators.erase(tail.operators.begin());
    if (tail.operators.empty()) {
      direct_arrow_lines_enabled_ = true;
      co_return DirectReadLinesSetup::enabled;
    }
    direct_metadata_usage_ = metadata_usage(args_, tail);
    if (not direct_metadata_usage_.any()) {
      direct_arrow_lines_enabled_ = true;
    }
    if (direct_metadata_usage_.any()) {
      auto replacements = direct_metadata_replacements(args_);
      if (not tail.replace_dollar_vars(replacements)) {
        co_return DirectReadLinesSetup::fallback;
      }
      direct_metadata_drop_fields_.push_back(
        direct_metadata_drop_field(args_.parser.source));
    }
    if (not tail.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      co_return DirectReadLinesSetup::error;
    }
    direct_tail_active_ = true;
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(tail));
    co_return DirectReadLinesSetup::enabled;
  }

  auto ensure_durable_consumer(OpCtx& ctx) -> Task<Option<std::string>> {
    TENZIR_ASSERT(args_.durable);
    auto stream_options = jsOptions{};
    jsOptions_Init(&stream_options);
    stream_options.Stream.Info.SubjectsFilter = args_.subject.inner.c_str();
    auto* raw_streams = static_cast<jsStreamNamesList*>(nullptr);
    auto js_error = jsErrCode{};
    auto status = co_await spawn_blocking([&] {
      return js_StreamNames(&raw_streams, js_.get(), &stream_options,
                            &js_error);
    });
    auto streams = js_stream_names_list_ptr{raw_streams};
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to resolve NATS stream for "
                                        "durable consumer")
                        .primary(args_.subject.source)
                        .note("JetStream error code: {}",
                              static_cast<int>(js_error)),
                      status, ctx.dh());
      co_return None{};
    }
    if (streams->Count == 0) {
      diagnostic::error("no NATS stream matches subject")
        .primary(args_.subject.source)
        .emit(ctx);
      co_return None{};
    }
    if (streams->Count > 1) {
      diagnostic::error("multiple NATS streams match subject")
        .primary(args_.subject.source)
        .note("matched {} streams", streams->Count)
        .emit(ctx);
      co_return None{};
    }
    auto stream = std::string{streams->List[0]};
    auto* raw_consumer = static_cast<jsConsumerInfo*>(nullptr);
    status = co_await spawn_blocking([&] {
      return js_GetConsumerInfo(&raw_consumer, js_.get(), stream.c_str(),
                                args_.durable->inner.c_str(), nullptr,
                                &js_error);
    });
    auto consumer = js_consumer_info_ptr{raw_consumer};
    if (status == NATS_OK) {
      direct_ack_all_ = direct_parser_ == DirectParser::read_lines
                        and consumer->Config
                        and consumer->Config->AckPolicy == js_AckAll;
      co_return stream;
    }
    if (status != NATS_NOT_FOUND) {
      emit_nats_error(diagnostic::error("failed to inspect NATS durable "
                                        "consumer")
                        .primary(args_.durable->source)
                        .note("JetStream error code: {}",
                              static_cast<int>(js_error)),
                      status, ctx.dh());
      co_return None{};
    }
    auto config = jsConsumerConfig{};
    jsConsumerConfig_Init(&config);
    config.Name = args_.durable->inner.c_str();
    config.Durable = args_.durable->inner.c_str();
    config.FilterSubject = args_.subject.inner.c_str();
    config.AckPolicy = desired_ack_policy();
    config.MaxAckPending = detail::narrow_cast<int>(args_.queue_capacity);
    status = co_await spawn_blocking([&] {
      return js_AddConsumer(nullptr, js_.get(), stream.c_str(), &config,
                            nullptr, &js_error);
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to create NATS durable "
                                        "consumer")
                        .primary(args_.durable->source)
                        .note("JetStream error code: {}",
                              static_cast<int>(js_error)),
                      status, ctx.dh());
      co_return None{};
    }
    direct_ack_all_ = config.AckPolicy == js_AckAll;
    co_return stream;
  }

  auto acknowledge(natsMsg* msg, OpCtx& ctx) -> bool {
    auto status = natsMsg_Ack(msg, nullptr);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::warning("failed to acknowledge NATS message")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      return false;
    }
    return true;
  }

  auto flush_acknowledgements(OpCtx& ctx) -> Task<void> {
    auto status = co_await spawn_blocking([this] {
      return natsConnection_FlushTimeout(connection_.get(),
                                         shutdown_flush_timeout_ms);
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::warning("failed to flush NATS "
                                          "acknowledgement")
                        .primary(args_.subject.source),
                      status, ctx.dh());
    }
  }

  auto acknowledge_direct_pending(OpCtx& ctx) -> void {
    if (direct_ack_all_) {
      if (not pending_direct_.empty()) {
        acknowledge(pending_direct_.back().get(), ctx);
        pending_direct_.clear();
      }
      return;
    }
    for (auto& msg : pending_direct_) {
      acknowledge(msg.get(), ctx);
    }
    pending_direct_.clear();
  }

  auto schedule_direct_flush_if_needed(
    series_builder::YieldReadyResult const& result, OpCtx& ctx) -> void {
    if (not result.wait_for or direct_flush_scheduled_) {
      return;
    }
    direct_flush_scheduled_ = true;
    ctx.spawn_task(schedule_direct_flush(source_, result.wait_for.unwrap()));
  }

  auto push_direct_slice(table_slice slice, Push<table_slice>& push, OpCtx& ctx)
    -> Task<bool> {
    if (direct_tail_active_) {
      auto sub = ctx.get_sub(caf::none);
      if (not sub) {
        co_return false;
      }
      auto& tail = as<SubHandle<table_slice>>(*sub);
      auto push_result = co_await tail.push(std::move(slice));
      co_return not push_result.is_err();
    }
    co_await push(std::move(slice));
    co_return true;
  }

  auto push_direct_result(series_builder::YieldReadyResult result,
                          Push<table_slice>& push, OpCtx& ctx) -> Task<void> {
    auto const had_slices = not result.slices.empty();
    for (auto& slice : result.slices) {
      if (not co_await push_direct_slice(std::move(slice), push, ctx)) {
        for (auto& msg : pending_direct_) {
          natsMsg_Nak(msg.get(), nullptr);
        }
        pending_direct_.clear();
        done_ = true;
        request_stop();
        co_return;
      }
    }
    if (had_slices) {
      acknowledge_direct_pending(ctx);
      direct_flush_scheduled_ = false;
    }
    schedule_direct_flush_if_needed(result, ctx);
  }

  auto direct_line_count() const -> int64_t {
    return direct_arrow_lines_enabled_ ? direct_arrow_line_count_
                                       : direct_read_lines_.length();
  }

  auto finish_direct_arrow_lines() -> table_slice {
    auto array = std::shared_ptr<arrow::Array>{};
    check(direct_arrow_lines_.Finish(&array));
    auto ty = direct_read_lines_type();
    auto batch = arrow::RecordBatch::Make(
      ty.to_arrow_schema(), direct_arrow_line_count_, {std::move(array)});
    direct_arrow_line_count_ = 0;
    direct_arrow_oldest_ = None{};
    return table_slice{std::move(batch), std::move(ty)};
  }

  auto flush_direct_arrow_lines(Push<table_slice>& push, OpCtx& ctx,
                                bool force = false) -> Task<void> {
    auto const length = detail::narrow_cast<uint64_t>(direct_arrow_line_count_);
    if (length == 0) {
      direct_arrow_oldest_ = None{};
      co_return;
    }
    auto const now = series_builder::clock::now();
    auto const desired_size
      = std::min(args_.batch_size, defaults::import::table_slice_size);
    auto ready = force or length >= desired_size;
    if (not ready and not direct_arrow_oldest_) {
      direct_arrow_oldest_ = now;
      schedule_direct_flush_if_needed(
        {.wait_for = defaults::import::batch_timeout}, ctx);
      co_return;
    }
    if (not ready) {
      auto const waiting = now - *direct_arrow_oldest_;
      ready = waiting >= defaults::import::batch_timeout;
      if (not ready) {
        schedule_direct_flush_if_needed(
          {.wait_for = defaults::import::batch_timeout - waiting}, ctx);
        co_return;
      }
    }
    auto slice = finish_direct_arrow_lines();
    if (not co_await push_direct_slice(std::move(slice), push, ctx)) {
      for (auto& msg : pending_direct_) {
        natsMsg_Nak(msg.get(), nullptr);
      }
      pending_direct_.clear();
      done_ = true;
      request_stop();
      co_return;
    }
    acknowledge_direct_pending(ctx);
    direct_flush_scheduled_ = false;
  }

  auto flush_direct_read_lines(Push<table_slice>& push, OpCtx& ctx,
                               bool force = false) -> Task<void> {
    if (direct_arrow_lines_enabled_) {
      co_await flush_direct_arrow_lines(push, ctx, force);
      co_return;
    }
    if (force) {
      for (auto& slice :
           direct_read_lines_.finish_as_table_slice(direct_read_lines_schema)) {
        if (not co_await push_direct_slice(std::move(slice), push, ctx)) {
          for (auto& msg : pending_direct_) {
            natsMsg_Nak(msg.get(), nullptr);
          }
          pending_direct_.clear();
          done_ = true;
          request_stop();
          co_return;
        }
      }
      acknowledge_direct_pending(ctx);
      direct_flush_scheduled_ = false;
      co_return;
    }
    auto result = direct_read_lines_.yield_ready(
      direct_read_lines_schema, series_builder::clock::now(),
      std::min(args_.batch_size, defaults::import::table_slice_size));
    co_await push_direct_result(std::move(result), push, ctx);
  }

  auto close_direct_tail(OpCtx& ctx) -> Task<void> {
    if (not direct_tail_active_ or direct_tail_closing_) {
      done_ = true;
      request_stop();
      co_return;
    }
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      direct_tail_finished_ = true;
      done_ = true;
      request_stop();
      co_return;
    }
    direct_tail_closing_ = true;
    auto& tail = as<SubHandle<table_slice>>(*sub);
    co_await tail.close();
  }

  auto emit_metadata_string(record_ref metadata, std::string_view field,
                            char const* value) -> void {
    if (value and *value) {
      metadata.field(field, std::string_view{value});
    } else {
      metadata.field(field, caf::none);
    }
  }

  auto emit_direct_metadata(record_ref row, natsMsg* msg,
                            js_msg_meta_data_ptr const& meta,
                            Option<record> const& headers) -> void {
    if (not direct_metadata_usage_.any()) {
      return;
    }
    auto metadata = row.field(direct_metadata_field).record();
    if (direct_metadata_usage_.subject) {
      emit_metadata_string(metadata, "subject", natsMsg_GetSubject(msg));
    }
    if (direct_metadata_usage_.reply) {
      emit_metadata_string(metadata, "reply", natsMsg_GetReply(msg));
    }
    if (direct_metadata_usage_.headers) {
      metadata.field("headers", headers ? data{*headers} : data{record{}});
    }
    auto emit_js_string
      = [&](bool enabled, std::string_view field, char const* value) {
          if (enabled) {
            emit_metadata_string(metadata, field, value);
          }
        };
    auto emit_js_data = [&](bool enabled, std::string_view field, auto value) {
      if (not enabled) {
        return;
      }
      if (meta) {
        metadata.field(field, value);
      } else {
        metadata.field(field, caf::none);
      }
    };
    emit_js_string(direct_metadata_usage_.stream, "stream",
                   meta ? meta->Stream : nullptr);
    emit_js_string(direct_metadata_usage_.consumer, "consumer",
                   meta ? meta->Consumer : nullptr);
    emit_js_data(direct_metadata_usage_.stream_sequence, "stream_sequence",
                 meta ? meta->Sequence.Stream : uint64_t{0});
    emit_js_data(direct_metadata_usage_.consumer_sequence, "consumer_sequence",
                 meta ? meta->Sequence.Consumer : uint64_t{0});
    emit_js_data(direct_metadata_usage_.num_delivered, "num_delivered",
                 meta ? meta->NumDelivered : uint64_t{0});
    emit_js_data(direct_metadata_usage_.num_pending, "num_pending",
                 meta ? meta->NumPending : uint64_t{0});
    emit_js_data(direct_metadata_usage_.timestamp, "timestamp",
                 meta ? time{} + duration{meta->Timestamp} : time{});
  }

  auto emit_direct_line(std::string_view line, natsMsg* msg,
                        js_msg_meta_data_ptr const& meta,
                        Option<record> const& headers, OpCtx& ctx) -> bool {
    if (direct_arrow_lines_enabled_) {
      check(direct_arrow_lines_.Append(line));
      ++direct_arrow_line_count_;
      return true;
    }
    if (not arrow::util::ValidateUTF8(line)) {
      diagnostic::warning("got invalid UTF-8")
        .primary(args_.parser.source)
        .hint("use a parser subpipeline with `read_lines binary=true` if you "
              "are reading binary data")
        .emit(ctx);
      return false;
    }
    auto row = direct_read_lines_.record();
    row.field("line", line);
    emit_direct_metadata(row, msg, meta, headers);
    return true;
  }

  auto validate_direct_arrow_lines(natsMsg* msg, OpCtx& ctx) -> bool {
    auto const size = natsMsg_GetDataLength(msg);
    if (size <= 0) {
      return true;
    }
    auto const* const data = natsMsg_GetData(msg);
    auto const* begin = data;
    auto const* const end = data + size;
    auto validate = [&](std::string_view line) {
      if (arrow::util::ValidateUTF8(line)) {
        return true;
      }
      diagnostic::warning("got invalid UTF-8")
        .primary(args_.parser.source)
        .hint("use a parser subpipeline with `read_lines binary=true` if you "
              "are reading binary data")
        .emit(ctx);
      return false;
    };
    for (auto const* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      if (not validate(std::string_view{begin, current})) {
        return false;
      }
      if (*current == '\r') {
        auto const* next = current + 1;
        if (next != end and *next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    return begin == end or validate(std::string_view{begin, end});
  }

  auto append_direct_read_lines(natsMsg* msg, OpCtx& ctx) -> bool {
    auto const size = natsMsg_GetDataLength(msg);
    if (size <= 0) {
      return true;
    }
    if (direct_arrow_lines_enabled_
        and not validate_direct_arrow_lines(msg, ctx)) {
      return false;
    }
    auto meta = direct_metadata_usage_.jetstream() ? message_metadata(msg)
                                                   : js_msg_meta_data_ptr{};
    auto headers = direct_metadata_usage_.headers
                     ? Option<record>{message_headers(msg)}
                     : None{};
    auto const* const data = natsMsg_GetData(msg);
    auto const* begin = data;
    auto const* const end = data + size;
    for (auto const* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      if (not emit_direct_line(std::string_view{begin, current}, msg, meta,
                               headers, ctx)) {
        return false;
      }
      if (*current == '\r') {
        auto const* next = current + 1;
        if (next != end and *next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    if (begin != end) {
      return emit_direct_line(std::string_view{begin, end}, msg, meta, headers,
                              ctx);
    }
    return true;
  }

  auto process_read_lines_message(nats_msg_ptr msg, Push<table_slice>& push,
                                  OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(msg);
    if (args_.count and received_ >= args_.count->inner) {
      natsMsg_Nak(msg.get(), nullptr);
      request_stop(PendingMessages::keep);
      co_return;
    }
    ++received_;
    auto const before = direct_line_count();
    auto const size = natsMsg_GetDataLength(msg.get());
    if (size > 0) {
      read_bytes_counter_.add(static_cast<size_t>(size));
    }
    if (not append_direct_read_lines(msg.get(), ctx)) {
      while (direct_read_lines_.length() > before) {
        direct_read_lines_.remove_last();
      }
      if (direct_ack_all_ and not pending_direct_.empty()) {
        co_await flush_direct_read_lines(push, ctx, true);
      }
      natsMsg_Nak(msg.get(), nullptr);
      if (direct_ack_all_) {
        done_ = true;
        request_stop(PendingMessages::keep);
      }
      co_return;
    }
    if (direct_line_count() == before) {
      if (direct_ack_all_ and not pending_direct_.empty()) {
        pending_direct_.push_back(std::move(msg));
        co_await flush_direct_read_lines(push, ctx, true);
      } else {
        acknowledge(msg.get(), ctx);
      }
    } else {
      pending_direct_.push_back(std::move(msg));
      auto const force_flush = args_.count and received_ >= args_.count->inner;
      co_await flush_direct_read_lines(push, ctx, force_flush);
    }
    if (args_.count and received_ >= args_.count->inner
        and pending_direct_.empty()) {
      co_await flush_acknowledgements(ctx);
      co_await close_direct_tail(ctx);
    }
  }

  auto process_message(nats_msg_ptr msg, OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(msg);
    if (args_.count and received_ >= args_.count->inner) {
      natsMsg_Nak(msg.get(), nullptr);
      request_stop(PendingMessages::keep);
      co_return;
    }
    auto const msg_id = next_msg_id_++;
    ++received_;
    auto pipeline = [&] {
      if (parser_template_) {
        return *parser_template_;
      }
      return args_.parser.inner;
    }();
    if (not parser_template_) {
      auto env = substitute_ctx::env_t{};
      add_metadata_to_env(args_, parser_metadata_usage_, msg.get(), env);
      auto sub_result
        = pipeline.substitute(substitute_ctx{base_ctx{ctx}, &env}, true);
      if (not sub_result) {
        natsMsg_Nak(msg.get(), nullptr);
        co_return;
      }
    }
    auto chunk = make_payload_chunk(msg.get());
    if (not chunk) {
      diagnostic::error("failed to allocate NATS message payload")
        .primary(args_.subject.source)
        .emit(ctx);
      natsMsg_Nak(msg.get(), nullptr);
      co_return;
    }
    read_bytes_counter_.add(chunk->size());
    auto key = data{detail::narrow_cast<int64_t>(msg_id)};
    pending_.emplace(msg_id, std::move(msg));
    auto& sub = co_await ctx.spawn_sub<chunk_ptr>(key, std::move(pipeline));
    auto push_result = co_await sub.push(std::move(chunk));
    if (push_result.is_err()) {
      natsMsg_Nak(pending_.at(msg_id).get(), nullptr);
      pending_.erase(msg_id);
      co_await sub.close();
      co_return;
    }
    co_await sub.close();
  }

  auto request_stop(PendingMessages pending_messages = PendingMessages::nak)
    -> void {
    if (not source_.not_moved_from()) {
      return;
    }
    source_->stopping.store(true, std::memory_order_release);
    while (auto message = source_->queue.try_dequeue()) {
      match(
        std::move(*message),
        [&](IncomingMessage item) {
          // Returning the slot keeps the configured data-message capacity
          // separate from the reserved completion slot.
          source_->message_slots.signal();
          natsMsg_Nak(item.msg.get(), nullptr);
        },
        [](SubscriptionComplete) {},
        [](FlushDirect) {});
    }
    if (pending_messages == PendingMessages::nak) {
      for (auto& [_, msg] : pending_) {
        natsMsg_Nak(msg.get(), nullptr);
      }
      pending_.clear();
      for (auto& msg : pending_direct_) {
        natsMsg_Nak(msg.get(), nullptr);
      }
      pending_direct_.clear();
    }
    if (pending_messages == PendingMessages::nak
        or (pending_.empty() and pending_direct_.empty())) {
      subscription_.reset();
    }
  }

  auto maybe_finish() -> void {
    if (args_.count and received_ >= args_.count->inner and pending_.empty()
        and pending_direct_.empty()) {
      if (direct_tail_active_ and not direct_tail_finished_) {
        return;
      }
      done_ = true;
      request_stop();
      return;
    }
    if (subscription_failed_ and pending_.empty() and pending_direct_.empty()
        and source_->queue.empty()) {
      if (direct_tail_active_ and not direct_tail_finished_) {
        return;
      }
      done_ = true;
      request_stop();
    }
  }

  FromNatsArgs args_;
  nats_options_ptr options_;
  nats_connection_ptr connection_;
  js_ctx_ptr js_;
  mutable Arc<SourceState> source_;
  nats_subscription_ptr subscription_;
  std::unordered_map<uint64_t, nats_msg_ptr> pending_;
  std::vector<nats_msg_ptr> pending_direct_;
  MetricsCounter read_bytes_counter_;
  Option<ir::pipeline> parser_template_ = None{};
  series_builder direct_read_lines_;
  arrow::StringBuilder direct_arrow_lines_{arrow_memory_pool()};
  int64_t direct_arrow_line_count_ = 0;
  Option<series_builder::clock::time_point> direct_arrow_oldest_ = None{};
  MetadataUsage direct_metadata_usage_;
  MetadataUsage parser_metadata_usage_;
  std::vector<ast::field_path> direct_metadata_drop_fields_;
  uint64_t next_msg_id_ = 0;
  uint64_t received_ = 0;
  DirectParser direct_parser_ = DirectParser::none;
  bool direct_flush_scheduled_ = false;
  bool direct_arrow_lines_enabled_ = false;
  bool direct_ack_all_ = false;
  bool direct_tail_active_ = false;
  bool direct_tail_closing_ = false;
  bool direct_tail_finished_ = false;
  bool subscription_failed_ = false;
  bool done_ = false;
};

class FromNatsPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_nats";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromNatsArgs, FromNats>{};
    auto subject_arg = d.positional("subject", &FromNatsArgs::subject);
    auto url_arg = d.named("url", &FromNatsArgs::url);
    d.named("durable", &FromNatsArgs::durable);
    auto count_arg = d.named("count", &FromNatsArgs::count);
    auto tls_arg = d.named("tls", &FromNatsArgs::tls, "record");
    auto auth_arg = d.named("auth", &FromNatsArgs::auth, "record");
    auto batch_size_arg
      = d.named_optional("_batch_size", &FromNatsArgs::batch_size);
    auto queue_capacity_arg
      = d.named_optional("_queue_capacity", &FromNatsArgs::queue_capacity);
    auto parser_arg
      = d.pipeline(&FromNatsArgs::parser,
                   {{"subject", &FromNatsArgs::subject_var},
                    {"reply", &FromNatsArgs::reply_var},
                    {"headers", &FromNatsArgs::headers_var},
                    {"stream", &FromNatsArgs::stream_var},
                    {"consumer", &FromNatsArgs::consumer_var},
                    {"stream_sequence", &FromNatsArgs::stream_sequence_var},
                    {"consumer_sequence", &FromNatsArgs::consumer_sequence_var},
                    {"num_delivered", &FromNatsArgs::num_delivered_var},
                    {"num_pending", &FromNatsArgs::num_pending_var},
                    {"timestamp", &FromNatsArgs::timestamp_var}});
    d.operator_location(&FromNatsArgs::op);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto subject, ctx.get(subject_arg));
      if (subject.inner.empty()) {
        diagnostic::error("`subject` must not be empty")
          .primary(subject.source)
          .emit(ctx);
      }
      if (auto url = ctx.get(url_arg);
          url and not url->inner.is_all_literal()) {
        // Managed secrets are resolved at runtime.
      }
      if (auto count = ctx.get(count_arg); count and count->inner == 0) {
        diagnostic::error("`count` must be greater than zero")
          .primary(ctx.get_location(count_arg).value_or(location::unknown))
          .emit(ctx);
      }
      if (auto batch_size = ctx.get(batch_size_arg); batch_size) {
        if (*batch_size == 0) {
          diagnostic::error("`_batch_size` must be greater than zero")
            .primary(
              ctx.get_location(batch_size_arg).value_or(location::unknown))
            .emit(ctx);
        }
        if (*batch_size
            > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
          diagnostic::error("`_batch_size` must fit into a 32-bit integer")
            .primary(
              ctx.get_location(batch_size_arg).value_or(location::unknown))
            .emit(ctx);
        }
      }
      if (auto queue_capacity = ctx.get(queue_capacity_arg); queue_capacity) {
        if (*queue_capacity == 0) {
          diagnostic::error("`_queue_capacity` must be greater than zero")
            .primary(
              ctx.get_location(queue_capacity_arg).value_or(location::unknown))
            .emit(ctx);
        }
        if (*queue_capacity > max_queue_capacity) {
          diagnostic::error("`_queue_capacity` must fit into a 32-bit integer")
            .primary(
              ctx.get_location(queue_capacity_arg).value_or(location::unknown))
            .emit(ctx);
        }
      }
      if (auto count = ctx.get(count_arg); count) {
        if (count->inner
            > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
          diagnostic::error("`count` must fit into a 32-bit integer")
            .primary(ctx.get_location(count_arg).value_or(location::unknown))
            .emit(ctx);
        }
      }
      if (auto tls_val = ctx.get(tls_arg)) {
        auto tls
          = tls_options{*tls_val, {.tls_default = true, .is_server = false}};
        if (auto valid = tls.validate(ctx); not valid) {
          return {};
        }
      }
      if (auto auth_val = ctx.get(auth_arg)) {
        if (not validate_auth_record(Option<located<data>>{*auth_val}, ctx)) {
          return {};
        }
      }
      TRY(auto parser, ctx.get(parser_arg));
      auto output = parser.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(parser.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::nats

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nats::FromNatsPlugin)
