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
#include <tenzir/atomic.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/coro/BoundedQueue.h>
#include <folly/fibers/Semaphore.h>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

namespace tenzir::plugins::nats {

namespace {

using namespace tenzir::si_literals;

constexpr auto default_batch_size = uint64_t{1_Ki / 8};
constexpr auto default_queue_capacity = uint64_t{1_Ki};
constexpr auto max_queue_capacity
  = uint64_t{std::numeric_limits<uint32_t>::max()} - 1;
constexpr auto shutdown_flush_timeout_ms = int64_t{5_k};

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

using SourceMessage = variant<IncomingMessage, SubscriptionComplete>;
using SourceQueue = folly::coro::BoundedQueue<SourceMessage>;

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
    : queue{detail::narrow_cast<uint32_t>(capacity + 1)},
      message_slots{capacity} {
  }

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

auto add_metadata_to_env(FromNatsArgs const& args, natsMsg* msg,
                         substitute_ctx::env_t& env) -> void {
  env[args.subject_var] = get_optional_c_string(natsMsg_GetSubject(msg));
  env[args.reply_var] = get_optional_c_string(natsMsg_GetReply(msg));
  env[args.headers_var] = message_headers(msg);
  auto meta = message_metadata(msg);
  if (not meta) {
    env[args.stream_var] = caf::none;
    env[args.consumer_var] = caf::none;
    env[args.stream_sequence_var] = caf::none;
    env[args.consumer_sequence_var] = caf::none;
    env[args.num_delivered_var] = caf::none;
    env[args.num_pending_var] = caf::none;
    env[args.timestamp_var] = caf::none;
    return;
  }
  env[args.stream_var] = get_optional_c_string(meta->Stream);
  env[args.consumer_var] = get_optional_c_string(meta->Consumer);
  env[args.stream_sequence_var] = meta->Sequence.Stream;
  env[args.consumer_sequence_var] = meta->Sequence.Consumer;
  env[args.num_delivered_var] = meta->NumDelivered;
  env[args.num_pending_var] = meta->NumPending;
  env[args.timestamp_var] = time{} + duration{meta->Timestamp};
}

auto set_auth_value(auth_config& auth, std::string const& key,
                    std::string value) -> void {
  if (key == "user") {
    auth.user = std::move(value);
  } else if (key == "password") {
    auth.password = std::move(value);
  } else if (key == "token") {
    auth.token = std::move(value);
  } else if (key == "credentials") {
    auth.credentials = std::move(value);
  } else if (key == "seed") {
    auth.seed = std::move(value);
  } else if (key == "credentials_memory") {
    auth.credentials_memory = std::move(value);
  }
}

auto resolve_connection_config(OpCtx& ctx, FromNatsArgs const& args)
  -> Task<Option<connection_config>> {
  auto result = connection_config{};
  auto requests = std::vector<secret_request>{};
  auto const& url = args.url ? args.url->inner : default_url();
  auto const url_loc = args.url ? args.url->source : location::unknown;
  requests.push_back(
    make_secret_request("url", url, url_loc, result.url, ctx.dh()));
  if (args.auth) {
    if (auto const* auth = try_as<record>(&args.auth->inner)) {
      for (auto const& [key, value] : *auth) {
        match(
          value,
          [&](std::string const& str) {
            set_auth_value(result.auth, key, str);
          },
          [&](secret const& sec) {
            requests.emplace_back(
              sec, args.auth->source,
              [&result, key, loc = args.auth->source, &dh = ctx.dh()](
                resolved_secret_value value) -> failure_or<void> {
                TRY(auto view, value.utf8_view(key, loc, dh));
                set_auth_value(result.auth, key, std::string{view});
                return {};
              });
          },
          [](auto const&) {
            TENZIR_UNREACHABLE();
          });
      }
    }
  }
  if (auto ok = co_await ctx.resolve_secrets(std::move(requests));
      ok.is_error()) {
    co_return None{};
  }
  if (result.url.empty()) {
    diagnostic::error("`url` must not be empty").primary(url_loc).emit(ctx);
    co_return None{};
  }
  co_return result;
}

class FromNats final : public Operator<void, table_slice> {
public:
  explicit FromNats(FromNatsArgs args)
    : args_{std::move(args)}, source_{std::in_place, args_.queue_capacity} {
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
    auto resolved = co_await resolve_connection_config(ctx, args_);
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
    sub_options.Config.AckPolicy = js_AckExplicit;
    auto const* durable
      = args_.durable ? args_.durable->inner.c_str() : nullptr;
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
    co_return co_await source_->queue.dequeue();
  }

  auto process_task(Any result, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    auto* message = result.try_as<SourceMessage>();
    if (not message) {
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](IncomingMessage item) -> Task<void> {
        source_->message_slots.signal();
        if (done_) {
          natsMsg_Nak(item.msg.get(), nullptr);
          co_return;
        }
        co_await process_message(std::move(item.msg), ctx);
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
      });
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx&) -> Task<void> override {
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    auto msg_id = detail::narrow_cast<uint64_t>(as<int64_t>(key));
    auto it = pending_.find(msg_id);
    if (it == pending_.end()) {
      co_return;
    }
    auto status = natsMsg_Ack(it->second.get(), nullptr);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::warning("failed to acknowledge NATS message")
                        .primary(args_.subject.source),
                      status, ctx.dh());
    }
    pending_.erase(it);
    if (args_.count and received_ >= args_.count->inner and pending_.empty()) {
      if (status == NATS_OK) {
        status = co_await spawn_blocking([this] {
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
    config.AckPolicy = js_AckExplicit;
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
    co_return stream;
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
    auto pipeline = args_.parser.inner;
    auto env = substitute_ctx::env_t{};
    add_metadata_to_env(args_, msg.get(), env);
    auto sub_result
      = pipeline.substitute(substitute_ctx{base_ctx{ctx}, &env}, true);
    if (not sub_result) {
      natsMsg_Nak(msg.get(), nullptr);
      co_return;
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
        [](SubscriptionComplete) {});
    }
    if (pending_messages == PendingMessages::nak) {
      for (auto& [_, msg] : pending_) {
        natsMsg_Nak(msg.get(), nullptr);
      }
      pending_.clear();
    }
    if (pending_messages == PendingMessages::nak or pending_.empty()) {
      subscription_.reset();
    }
  }

  auto maybe_finish() -> void {
    if (args_.count and received_ >= args_.count->inner and pending_.empty()) {
      done_ = true;
      request_stop();
      return;
    }
    if (subscription_failed_ and pending_.empty() and source_->queue.empty()) {
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
  MetricsCounter read_bytes_counter_;
  uint64_t next_msg_id_ = 0;
  uint64_t received_ = 0;
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
