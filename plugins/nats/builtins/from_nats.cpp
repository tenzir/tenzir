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
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/Exception.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/fibers/Semaphore.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace tenzir::plugins::nats {

namespace {

using namespace std::chrono_literals;
using namespace tenzir::si_literals;

constexpr auto default_batch_size = uint64_t{10_k};
constexpr auto default_queue_capacity = uint64_t{10_k};
constexpr auto default_batch_timeout = 100ms;
// Give downstream early-stop control messages a bounded window to arrive
// before we ACK a delivered batch and pull the next one.
constexpr auto ack_handoff_delay = 100us;
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

auto negative_acknowledge(natsMsg* msg) noexcept -> bool {
  return natsMsg_Nak(msg, nullptr) == NATS_OK;
}

struct AcknowledgePending {};

using SourceMessage
  = variant<IncomingMessage, SubscriptionComplete, AcknowledgePending>;
using SourceMessageQueue = folly::coro::BoundedQueue<SourceMessage>;

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

auto normal_completion(natsStatus status) -> bool {
  return status == NATS_MAX_DELIVERED_MSGS or status == NATS_TIMEOUT
         or status == NATS_NOT_FOUND or status == NATS_LIMIT_REACHED;
}

class SourceQueue {
public:
  explicit SourceQueue(uint64_t capacity)
    : messages_{detail::narrow_cast<uint32_t>(capacity + 2)},
      message_slots_{capacity} {
  }

  auto enqueue(natsMsg* msg) -> void {
    if (not accepting_.load(std::memory_order_acquire)) {
      reject(msg);
      return;
    }
    if (not message_slots_.try_wait()) {
      reject(msg);
      return;
    }
    if (messages_.try_enqueue(
          SourceMessage{IncomingMessage{nats_msg_ptr{msg}}})) {
      return;
    }
    release_message_slot();
    reject(msg);
  }

  auto enqueue(natsStatus status) -> void {
    if (not accepting_.load(std::memory_order_acquire)) {
      return;
    }
    if (status == NATS_OK or normal_completion(status)) {
      return;
    }
    if (terminal_completion_queued_.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    [[maybe_unused]] auto enqueued
      = messages_.try_enqueue(SourceMessage{SubscriptionComplete{status}});
    TENZIR_ASSERT(enqueued);
  }

  auto enqueue_acknowledge_pending() -> void {
    acknowledge_pending_queued_.store(true, std::memory_order_release);
    if (acknowledge_pending_wake_queued_.exchange(true,
                                                  std::memory_order_acq_rel)) {
      return;
    }
    [[maybe_unused]] auto enqueued
      = messages_.try_enqueue(SourceMessage{AcknowledgePending{}});
    TENZIR_ASSERT(enqueued);
  }

  auto dequeue() -> Task<SourceMessage> {
    while (true) {
      if (auto control = take_control()) {
        co_return std::move(*control);
      }
      if (auto message = take(co_await messages_.dequeue())) {
        co_return std::move(*message);
      }
    }
  }

  auto try_dequeue() -> Option<SourceMessage> {
    if (auto control = take_control()) {
      return control;
    }
    while (auto message = messages_.try_dequeue()) {
      if (auto result = take(std::move(*message))) {
        return result;
      }
    }
    return None{};
  }

  auto try_dequeue_for(duration timeout) -> Task<Option<SourceMessage>> {
    if (auto control = take_control()) {
      co_return std::move(*control);
    }
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    auto token = co_await folly::coro::co_current_cancellation_token;
    while (true) {
      try {
        auto message = co_await messages_.co_try_dequeue_for(
          std::chrono::duration_cast<folly::Duration>(timeout));
        if (auto result = take(std::move(message))) {
          co_return std::move(*result);
        }
        auto const now = std::chrono::steady_clock::now();
        if (now >= deadline) {
          co_return None{};
        }
        timeout = std::chrono::duration_cast<duration>(deadline - now);
      } catch (folly::OperationCancelled const&) {
        if (token.isCancellationRequested()) {
          throw;
        }
        co_return None{};
      }
    }
  }

  auto stop_accepting() -> void {
    accepting_.store(false, std::memory_order_release);
  }

  auto release_message_slot() -> void {
    message_slots_.signal();
  }

private:
  auto take_control() -> Option<SourceMessage> {
    if (acknowledge_pending_queued_.exchange(false,
                                             std::memory_order_acq_rel)) {
      return SourceMessage{AcknowledgePending{}};
    }
    return None{};
  }

  auto take(SourceMessage message) -> Option<SourceMessage> {
    if (try_as<AcknowledgePending>(&message)) {
      acknowledge_pending_wake_queued_.store(false, std::memory_order_release);
      return take_control();
    }
    return message;
  }

  static auto reject(natsMsg* msg) -> void {
    negative_acknowledge(msg);
    natsMsg_Destroy(msg);
  }

  // NATS invokes callbacks from library-owned threads. The queue reserves one
  // terminal and one ACK slot in addition to the bounded message slots so that
  // control messages can still wake the operator when the data queue is full.
  SourceMessageQueue messages_;
  folly::fibers::Semaphore message_slots_;
  Atomic<bool> accepting_ = true;
  Atomic<bool> terminal_completion_queued_ = false;
  Atomic<bool> acknowledge_pending_queued_ = false;
  Atomic<bool> acknowledge_pending_wake_queued_ = false;
};

struct SourceBatch {
  explicit SourceBatch(Arc<SourceQueue> source) : source_{std::move(source)} {
  }

  SourceBatch(SourceBatch const&) = delete;
  auto operator=(SourceBatch const&) -> SourceBatch& = delete;
  SourceBatch(SourceBatch&&) noexcept = default;
  auto operator=(SourceBatch&& other) noexcept -> SourceBatch& {
    if (this == &other) {
      return *this;
    }
    return_messages();
    source_ = std::move(other.source_);
    messages = std::move(other.messages);
    completion = std::move(other.completion);
    acknowledge_pending = other.acknowledge_pending;
    other.acknowledge_pending = false;
    return *this;
  }

  ~SourceBatch() {
    return_messages();
  }

  std::vector<nats_msg_ptr> messages;
  Option<SubscriptionComplete> completion = None{};
  bool acknowledge_pending = false;

private:
  auto return_messages() noexcept -> void {
    for (auto& msg : messages) {
      if (msg) {
        negative_acknowledge(msg.get());
        if (source_.not_moved_from()) {
          source_->release_message_slot();
        }
      }
    }
    messages.clear();
  }

  Arc<SourceQueue> source_;
};

void message_callback(natsConnection*, natsSubscription*, natsMsg* msg,
                      void* closure) {
  auto* queue = static_cast<SourceQueue*>(closure);
  TENZIR_ASSERT(queue);
  queue->enqueue(msg);
}

void complete_callback(natsConnection*, natsSubscription*, natsStatus status,
                       void* closure) {
  auto* queue = static_cast<SourceQueue*>(closure);
  TENZIR_ASSERT(queue);
  queue->enqueue(status);
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
    cleanup_sync();
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    read_bytes_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_nats"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsType::bytes);
    read_events_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_nats"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsType::events);
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
    auto const fetch_size = std::min(args_.batch_size, args_.queue_capacity);
    auto js_options = jsOptions{};
    jsOptions_Init(&js_options);
    js_options.PullSubscribeAsync.FetchSize
      = detail::narrow_cast<int>(fetch_size);
    js_options.PullSubscribeAsync.KeepAhead
      = detail::narrow_cast<int>(std::max<uint64_t>(1, fetch_size / 2));
    js_options.PullSubscribeAsync.CompleteHandler = complete_callback;
    js_options.PullSubscribeAsync.CompleteHandlerClosure = &*source_;
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

  auto post_commit(OpCtx& ctx) -> Task<void> override {
    auto const acknowledged = acknowledge_pending(ctx.dh());
    if (acknowledged and connection_) {
      co_await flush_acknowledgements(ctx);
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("received", received_);
    serde("done", done_);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    auto batch = SourceBatch{source_};
    auto first = co_await source_->dequeue();
    if (try_as<AcknowledgePending>(&first)) {
      co_await wait_for_downstream_stop();
    }
    if (not append_to_batch(batch, std::move(first))) {
      co_return batch;
    }
    auto const max_messages = detail::narrow_cast<size_t>(
      std::min(args_.batch_size, args_.queue_capacity));
    auto const started = std::chrono::steady_clock::now();
    while (batch.messages.size() < max_messages) {
      if (auto next = source_->try_dequeue()) {
        if (try_as<AcknowledgePending>(&*next)) {
          co_await wait_for_downstream_stop();
        }
        if (not append_to_batch(batch, std::move(*next))) {
          co_return batch;
        }
        continue;
      }
      auto const elapsed = std::chrono::duration_cast<duration>(
        std::chrono::steady_clock::now() - started);
      if (elapsed >= args_.batch_timeout) {
        break;
      }
      // Once we hold messages that will need an ACK after delivery, avoid
      // lingering for the full batch timeout. Otherwise short JetStream ACK
      // waits can redeliver messages before the batch reaches downstream.
      auto const timeout
        = std::min(args_.batch_timeout - elapsed, duration{ack_handoff_delay});
      auto next = co_await source_->try_dequeue_for(timeout);
      if (not next) {
        break;
      }
      if (try_as<AcknowledgePending>(&*next)) {
        co_await wait_for_downstream_stop();
      }
      if (not append_to_batch(batch, std::move(*next))) {
        co_return batch;
      }
    }
    co_return batch;
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto* batch = result.try_as<SourceBatch>();
    if (not batch) {
      co_return;
    }
    if (batch->acknowledge_pending) {
      auto const acknowledged = acknowledge_pending(ctx.dh());
      if (acknowledged and connection_) {
        co_await flush_acknowledgements(ctx);
      }
    }
    if (batch->messages.empty() and not batch->completion) {
      co_return;
    }
    auto accepted = std::vector<nats_msg_ptr>{};
    accepted.reserve(batch->messages.size());
    for (auto& msg : batch->messages) {
      if (done_) {
        needs_shutdown_flush_
          = negative_acknowledge(msg.get()) or needs_shutdown_flush_;
        source_->release_message_slot();
        msg.reset();
        continue;
      }
      if (args_.count and received_ >= args_.count->inner) {
        needs_shutdown_flush_
          = negative_acknowledge(msg.get()) or needs_shutdown_flush_;
        source_->release_message_slot();
        msg.reset();
        continue;
      }
      auto const size = natsMsg_GetDataLength(msg.get());
      if (size > 0) {
        read_bytes_counter_.add(static_cast<size_t>(size));
      }
      ++received_;
      accepted.push_back(std::move(msg));
    }
    batch->messages.clear();
    if (not accepted.empty()) {
      auto nak_guard = detail::scope_guard{[this, &accepted]() noexcept {
        for (auto& msg : accepted) {
          if (msg) {
            needs_shutdown_flush_
              = negative_acknowledge(msg.get()) or needs_shutdown_flush_;
            source_->release_message_slot();
          }
        }
      }};
      auto message_index = size_t{0};
      for (auto& slice : build_slices(accepted, ctx)) {
        auto const rows = detail::narrow_cast<size_t>(slice.rows());
        TENZIR_ASSERT(message_index + rows <= accepted.size());
        co_await push(std::move(slice));
        read_events_counter_.add(rows);
        mark_delivered(
          std::span<nats_msg_ptr>{accepted.data() + message_index, rows});
        for (auto i = size_t{0}; i < rows; ++i) {
          source_->release_message_slot();
        }
        message_index += rows;
      }
      TENZIR_ASSERT(message_index == accepted.size());
      nak_guard.disable();
      if (co_await maybe_stop_after_count(ctx)) {
        co_return;
      }
      schedule_ack_handoff();
    }
    if (batch->completion and not normal_completion(batch->completion->status)
        and batch->completion->status != NATS_OK) {
      emit_nats_error(diagnostic::error("NATS subscription ended with error")
                        .primary(args_.subject.source),
                      batch->completion->status, ctx.dh());
      done_ = true;
      co_await request_stop(ctx);
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    done_ = true;
    co_await request_stop(ctx);
  }

private:
  static auto append_to_batch(SourceBatch& batch, SourceMessage message)
    -> bool {
    if (auto* item = try_as<IncomingMessage>(&message)) {
      batch.messages.push_back(std::move(item->msg));
      return true;
    }
    if (auto* complete = try_as<SubscriptionComplete>(&message)) {
      batch.completion = *complete;
      return false;
    }
    if (try_as<AcknowledgePending>(&message)) {
      batch.acknowledge_pending = true;
      return false;
    }
    TENZIR_UNREACHABLE();
  }

  auto mark_delivered(std::span<nats_msg_ptr> messages) -> void {
    for (auto& msg : messages) {
      if (msg) {
        pending_acks_.push_back(std::move(msg));
      }
    }
  }

  auto wait_for_downstream_stop() const -> Task<void> {
    co_await sleep_for(ack_handoff_delay);
  }

  auto schedule_ack_handoff() -> void {
    if (done_ or pending_acks_.empty()) {
      return;
    }
    source_->enqueue_acknowledge_pending();
  }

  auto maybe_stop_after_count(OpCtx& ctx) -> Task<bool> {
    if (not args_.count or received_ < args_.count->inner) {
      co_return false;
    }
    auto const acknowledged = acknowledge_pending(ctx.dh());
    if (acknowledged and connection_) {
      co_await flush_acknowledgements(ctx);
    }
    done_ = true;
    co_await request_stop(ctx);
    co_return true;
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

  auto acknowledge(natsMsg* msg, diagnostic_handler& dh) const -> bool {
    auto status = natsMsg_Ack(msg, nullptr);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::warning("failed to acknowledge NATS message")
                        .primary(args_.subject.source),
                      status, dh);
      return false;
    }
    return true;
  }

  auto acknowledge_pending(diagnostic_handler& dh) -> bool {
    if (pending_acks_.empty()) {
      return false;
    }
    auto acknowledged = false;
    for (auto& msg : pending_acks_) {
      if (msg) {
        acknowledged = acknowledge(msg.get(), dh) or acknowledged;
      }
    }
    pending_acks_.clear();
    return acknowledged;
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

  auto unsubscribe_and_flush_subscription(OpCtx& ctx) -> Task<void> {
    if (not subscription_) {
      co_return;
    }
    auto status = co_await spawn_blocking([this] {
      auto unsubscribe_status
        = natsSubscription_Unsubscribe(subscription_.get());
      if (unsubscribe_status != NATS_OK or not connection_) {
        return unsubscribe_status;
      }
      return natsConnection_FlushTimeout(connection_.get(),
                                         shutdown_flush_timeout_ms);
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::warning("failed to stop NATS subscription")
                        .primary(args_.subject.source),
                      status, ctx.dh());
    }
  }

  auto cleanup_sync() noexcept -> void {
    if (not source_.not_moved_from()) {
      return;
    }
    source_->stop_accepting();
    auto needs_flush = std::exchange(needs_shutdown_flush_, false);
    if (subscription_) {
      auto const unsubscribe_status
        = natsSubscription_Unsubscribe(subscription_.get());
      needs_flush = unsubscribe_status == NATS_OK or needs_flush;
    }
    while (auto message = source_->try_dequeue()) {
      match(
        std::move(*message),
        [&](IncomingMessage item) {
          source_->release_message_slot();
          needs_flush = negative_acknowledge(item.msg.get()) or needs_flush;
        },
        [](SubscriptionComplete) {}, [](AcknowledgePending) {});
    }
    if (not pending_acks_.empty()) {
      for (auto& msg : pending_acks_) {
        if (msg) {
          needs_flush
            = natsMsg_Ack(msg.get(), nullptr) == NATS_OK or needs_flush;
        }
      }
      pending_acks_.clear();
    }
    if (needs_flush and connection_) {
      natsConnection_FlushTimeout(connection_.get(), shutdown_flush_timeout_ms);
    }
    subscription_.reset();
  }

  auto request_stop(OpCtx& ctx) -> Task<void> {
    if (not source_.not_moved_from()) {
      co_return;
    }
    source_->stop_accepting();
    auto needs_flush = std::exchange(needs_shutdown_flush_, false);
    // Remove interest before NAKing locally held messages. Otherwise JetStream
    // may redeliver them to this subscription while it is tearing down.
    if (subscription_) {
      co_await unsubscribe_and_flush_subscription(ctx);
    }
    while (auto message = source_->try_dequeue()) {
      match(
        std::move(*message),
        [&](IncomingMessage item) {
          source_->release_message_slot();
          needs_flush = negative_acknowledge(item.msg.get()) or needs_flush;
        },
        [](SubscriptionComplete) {}, [](AcknowledgePending) {});
    }
    needs_flush = acknowledge_pending(ctx.dh()) or needs_flush;
    if (needs_flush and connection_) {
      co_await flush_acknowledgements(ctx);
    }
    subscription_.reset();
  }

  FromNatsArgs args_;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;
  nats_options_ptr options_;
  nats_connection_ptr connection_;
  js_ctx_ptr js_;
  mutable Arc<SourceQueue> source_;
  nats_subscription_ptr subscription_;
  std::vector<nats_msg_ptr> pending_acks_;
  MetricsCounter read_bytes_counter_;
  MetricsCounter read_events_counter_;
  uint64_t received_ = 0;
  bool needs_shutdown_flush_ = false;
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
