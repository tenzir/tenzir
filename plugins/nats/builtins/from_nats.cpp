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
#include <tenzir/blob.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/coro/BoundedQueue.h>
#include <folly/fibers/Semaphore.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <memory>
#include <span>
#include <vector>

namespace tenzir::plugins::nats {

namespace {

using namespace std::chrono_literals;
using namespace tenzir::si_literals;

constexpr auto default_batch_size = uint64_t{10_k};
constexpr auto default_queue_capacity = uint64_t{10_k};
constexpr auto default_batch_timeout = 100ms;
constexpr auto max_queue_capacity = uint64_t{std::numeric_limits<int>::max()};
constexpr auto shutdown_flush_timeout_ms = int64_t{5_k};

struct FromNatsArgs {
  located<std::string> subject;
  Option<located<secret>> url;
  Option<located<std::string>> durable;
  Option<located<uint64_t>> count;
  Option<ast::field_path> metadata_field;
  Option<located<data>> tls;
  Option<located<data>> auth;
  uint64_t batch_size = default_batch_size;
  uint64_t queue_capacity = default_queue_capacity;
  duration batch_timeout = default_batch_timeout;
  location op;
};

struct IncomingMessage {
  nats_msg_ptr msg;
};

struct SubscriptionComplete {
  natsStatus status;
};

using SourceMessage = variant<IncomingMessage, SubscriptionComplete>;
using SourceQueue = folly::coro::BoundedQueue<SourceMessage>;

struct SourceBatch {
  std::vector<nats_msg_ptr> messages;
  Option<SubscriptionComplete> completion = None{};
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
    : queue{detail::narrow_cast<uint32_t>(capacity + 1)},
      message_slots{capacity} {
  }

  // The queue has `message_slots` plus one slot for the first terminal
  // completion.
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

auto optional_c_string(char const* str) -> data {
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

auto message_payload(natsMsg* msg) -> blob {
  auto const size = natsMsg_GetDataLength(msg);
  if (size <= 0) {
    return {};
  }
  auto const* bytes = reinterpret_cast<std::byte const*>(natsMsg_GetData(msg));
  return blob{std::span<const std::byte>{bytes, static_cast<size_t>(size)}};
}

template <class Record>
auto emit_metadata(Record& metadata, natsMsg* msg) -> void {
  auto meta = message_metadata(msg);
  metadata.field("subject").data(optional_c_string(natsMsg_GetSubject(msg)));
  metadata.field("reply").data(optional_c_string(natsMsg_GetReply(msg)));
  metadata.field("headers").data(data{message_headers(msg)});
  metadata.field("stream").data(meta ? optional_c_string(meta->Stream)
                                     : data{caf::none});
  metadata.field("consumer")
    .data(meta ? optional_c_string(meta->Consumer) : data{caf::none});
  metadata.field("stream_sequence")
    .data(meta ? data{meta->Sequence.Stream} : data{caf::none});
  metadata.field("consumer_sequence")
    .data(meta ? data{meta->Sequence.Consumer} : data{caf::none});
  metadata.field("num_delivered")
    .data(meta ? data{meta->NumDelivered} : data{caf::none});
  metadata.field("num_pending")
    .data(meta ? data{meta->NumPending} : data{caf::none});
  metadata.field("timestamp")
    .data(meta ? data{time{} + duration{meta->Timestamp}} : data{caf::none});
}

auto metadata_field_invalid(ast::field_path const& path) -> bool {
  auto const segments = path.path();
  if (segments.empty()) {
    return true;
  }
  return segments.front().id.name == "message";
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
    auto resolved
      = co_await resolve_connection_config(ctx, args_.url, args_.auth);
    if (not resolved) {
      done_ = true;
      co_return;
    }
    io_executor_ = ctx.io_executor();
    auto* evb = io_executor_->getEventBase();
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
    auto const fetch_size = uint64_t{1};
    auto js_options = jsOptions{};
    jsOptions_Init(&js_options);
    js_options.PullSubscribeAsync.FetchSize
      = detail::narrow_cast<int>(fetch_size);
    js_options.PullSubscribeAsync.KeepAhead
      = detail::narrow_cast<int>(std::max<uint64_t>(1, fetch_size / 2));
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
    if (not durable) {
      sub_options.Config.MaxAckPending
        = detail::narrow<int>(args_.queue_capacity);
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
    auto first = Option<SourceMessage>{None{}};
    if (pending_ack_) {
      first = co_await source_->queue.co_try_dequeue_for(
        std::chrono::duration_cast<folly::Duration>(args_.batch_timeout));
      if (not first) {
        co_return batch;
      }
    } else {
      first = co_await source_->queue.dequeue();
    }
    if (auto* item = try_as<IncomingMessage>(&*first)) {
      batch.messages.push_back(std::move(item->msg));
    } else if (auto* complete = try_as<SubscriptionComplete>(&*first)) {
      batch.completion = *complete;
      co_return batch;
    }
    co_return batch;
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto* batch = result.try_as<SourceBatch>();
    if (not batch) {
      co_return;
    }
    if (batch->messages.empty() and not batch->completion) {
      acknowledge_pending(ctx);
      co_return;
    }
    auto accepted = std::vector<nats_msg_ptr>{};
    accepted.reserve(batch->messages.size());
    for (auto& msg : batch->messages) {
      source_->message_slots.signal();
      if (done_) {
        natsMsg_Nak(msg.get(), nullptr);
        continue;
      }
      if (args_.count and received_ >= args_.count->inner) {
        natsMsg_Nak(msg.get(), nullptr);
        continue;
      }
      auto const size = natsMsg_GetDataLength(msg.get());
      if (size > 0) {
        read_bytes_counter_.add(static_cast<size_t>(size));
      }
      ++received_;
      accepted.push_back(std::move(msg));
    }
    if (not accepted.empty()) {
      auto acked = acknowledge_pending(ctx);
      TENZIR_ASSERT(in_flight_.empty());
      in_flight_ = std::move(accepted);
      auto nak_guard = detail::scope_guard{[this]() noexcept {
        for (auto& msg : in_flight_) {
          if (msg) {
            natsMsg_Nak(msg.get(), nullptr);
          }
        }
        in_flight_.clear();
      }};
      for (auto& msg : in_flight_) {
        if (not msg) {
          continue;
        }
        auto one = std::span<nats_msg_ptr const>{&msg, 1};
        for (auto& slice : build_slices(one, ctx)) {
          co_await push(std::move(slice));
        }
        pending_ack_ = std::move(msg);
      }
      in_flight_.clear();
      nak_guard.disable();
      if (args_.count and received_ >= args_.count->inner) {
        if (args_.count->inner == 1) {
          acked = acknowledge_pending(ctx) or acked;
        }
        if (acked) {
          co_await flush_acknowledgements(ctx);
        }
        done_ = true;
        request_stop();
        co_return;
      }
    }
    if (batch->completion and not normal_completion(batch->completion->status)
        and batch->completion->status != NATS_OK) {
      emit_nats_error(diagnostic::error("NATS subscription ended with error")
                        .primary(args_.subject.source),
                      batch->completion->status, ctx.dh());
      done_ = true;
      request_stop();
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
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
      if (not consumer->Config
          or consumer->Config->AckPolicy != js_AckExplicit) {
        diagnostic::error("NATS durable consumer must use explicit "
                          "acknowledgments")
          .primary(args_.durable->source)
          .note("consumer `{}` on stream `{}` uses ack policy `{}`",
                args_.durable->inner, stream,
                consumer->Config ? static_cast<int>(consumer->Config->AckPolicy)
                                 : -1)
          .emit(ctx);
        co_return None{};
      }
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
    config.MaxAckPending = detail::narrow<int>(args_.queue_capacity);
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

  auto build_slices(std::span<nats_msg_ptr const> messages, OpCtx& ctx)
    -> std::vector<table_slice> {
    auto opts = multi_series_builder::options{};
    opts.settings.default_schema_name = "tenzir.nats";
    opts.settings.desired_batch_size = detail::narrow_cast<size_t>(
      std::min(args_.batch_size, args_.queue_capacity));
    opts.settings.timeout
      = std::chrono::duration_cast<multi_series_builder::duration>(
        args_.batch_timeout);
    auto builder = multi_series_builder{opts, ctx.dh()};
    for (auto const& msg : messages) {
      auto event = builder.record();
      event.field("message").data(message_payload(msg.get()));
      if (args_.metadata_field) {
        auto metadata = event.field(*args_.metadata_field).record();
        emit_metadata(metadata, msg.get());
      }
    }
    return builder.finalize_as_table_slice();
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

  auto acknowledge_pending(OpCtx& ctx) -> bool {
    if (not pending_ack_) {
      return false;
    }
    auto const ok = acknowledge(pending_ack_.get(), ctx);
    pending_ack_.reset();
    return ok;
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

  auto request_stop() -> void {
    if (not source_.not_moved_from()) {
      return;
    }
    source_->stopping.store(true, std::memory_order_release);
    auto nacked = false;
    while (auto message = source_->queue.try_dequeue()) {
      match(
        std::move(*message),
        [&](IncomingMessage item) {
          source_->message_slots.signal();
          natsMsg_Nak(item.msg.get(), nullptr);
          nacked = true;
        },
        [](SubscriptionComplete) {});
    }
    if (pending_ack_) {
      natsMsg_Nak(pending_ack_.get(), nullptr);
      pending_ack_.reset();
      nacked = true;
    }
    for (auto& msg : in_flight_) {
      if (msg) {
        natsMsg_Nak(msg.get(), nullptr);
        nacked = true;
      }
    }
    in_flight_.clear();
    if (nacked and connection_) {
      std::ignore = natsConnection_FlushTimeout(connection_.get(),
                                                shutdown_flush_timeout_ms);
    }
    subscription_.reset();
  }

  FromNatsArgs args_;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;
  nats_options_ptr options_;
  nats_connection_ptr connection_;
  js_ctx_ptr js_;
  mutable Arc<SourceState> source_;
  nats_subscription_ptr subscription_;
  mutable nats_msg_ptr pending_ack_;
  std::vector<nats_msg_ptr> in_flight_;
  MetricsCounter read_bytes_counter_;
  uint64_t received_ = 0;
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
    auto metadata_field_arg
      = d.named("metadata_field", &FromNatsArgs::metadata_field);
    auto tls_arg = d.named("tls", &FromNatsArgs::tls, "record");
    auto auth_arg = d.named("auth", &FromNatsArgs::auth, "record");
    auto batch_size_arg
      = d.named_optional("_batch_size", &FromNatsArgs::batch_size);
    auto queue_capacity_arg
      = d.named_optional("_queue_capacity", &FromNatsArgs::queue_capacity);
    auto batch_timeout_arg
      = d.named_optional("_batch_timeout", &FromNatsArgs::batch_timeout);
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
      if (auto count = ctx.get(count_arg); count) {
        if (count->inner == 0) {
          diagnostic::error("`count` must be greater than zero")
            .primary(ctx.get_location(count_arg).value_or(location::unknown))
            .emit(ctx);
        }
        if (count->inner
            > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
          diagnostic::error("`count` must fit into a 32-bit integer")
            .primary(ctx.get_location(count_arg).value_or(location::unknown))
            .emit(ctx);
        }
      }
      if (auto metadata_field = ctx.get(metadata_field_arg);
          metadata_field and metadata_field_invalid(*metadata_field)) {
        diagnostic::error("`metadata_field` must not overlap with `message`")
          .primary(
            ctx.get_location(metadata_field_arg).value_or(location::unknown))
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
      if (auto batch_timeout = ctx.get(batch_timeout_arg); batch_timeout) {
        if (*batch_timeout <= duration::zero()) {
          diagnostic::error("`_batch_timeout` must be a positive duration")
            .primary(
              ctx.get_location(batch_timeout_arg).value_or(location::unknown))
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
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::nats

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nats::FromNatsPlugin)
