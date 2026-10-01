//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/CurrentExecutor.h>

#include <algorithm>
#include <limits>

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::amqp {

namespace {

constexpr auto message_queue_capacity = uint32_t{1024};
constexpr auto consume_timeout = 500ms;

struct FromAmqpArgs {
  located<secret> url;
  Option<located<uint64_t>> channel;
  Option<located<std::string>> exchange;
  Option<located<std::string>> routing_key;
  Option<located<std::string>> queue;
  Option<located<record>> options;
  Option<located<record>> queue_arguments;
  bool passive = false;
  bool durable = false;
  bool exclusive = false;
  bool no_auto_delete = false;
  bool no_local = false;
  bool ack = false;
  record plugin_config;
};

auto has_classic_or_quorum_queue_type(FromAmqpArgs const& args) -> bool {
  if (not args.queue_arguments) {
    return false;
  }
  auto type = args.queue_arguments->inner.find("x-queue-type");
  if (type == args.queue_arguments->inner.end()) {
    return false;
  }
  auto* name = try_as<std::string>(&type->second);
  return name and (*name == "classic" or *name == "quorum");
}

struct AmqpMessage {
  chunk_ptr chunk;
};

struct AmqpError {
  std::string message;
};

struct PeriodicTick {};

using FromAmqpEvent = variant<AmqpMessage, AmqpError>;
using FromAmqpQueue = folly::coro::BoundedQueue<FromAmqpEvent>;

/// Owns the storage referenced by the shallow RabbitMQ-C table view.
class AmqpFieldTable {
public:
  explicit AmqpFieldTable(const record& args) {
    keys_.reserve(args.size());
    string_values_.reserve(args.size());
    entries_.reserve(args.size());
    for (const auto& [key, value] : args) {
      keys_.push_back(key);
      auto entry = amqp_table_entry_t{
        .key = as_amqp_bytes(keys_.back()),
        .value = {},
      };
      entry.value = make_value(value);
      entries_.push_back(entry);
    }
    table_ = amqp_table_t{
      .num_entries = detail::narrow<int>(entries_.size()),
      .entries = entries_.empty() ? nullptr : entries_.data(),
    };
  }

  AmqpFieldTable(const AmqpFieldTable&) = delete;
  auto operator=(const AmqpFieldTable&) -> AmqpFieldTable& = delete;
  AmqpFieldTable(AmqpFieldTable&&) = delete;
  auto operator=(AmqpFieldTable&&) -> AmqpFieldTable& = delete;

  auto view() const -> amqp_table_t {
    return table_;
  }

private:
  auto make_value(const data& value) -> amqp_field_value_t {
    return match(
      value,
      [](bool x) {
        return amqp_field_value_t{
          .kind = AMQP_FIELD_KIND_BOOLEAN,
          .value = {.boolean = as_amqp_bool(x)},
        };
      },
      [](int64_t x) {
        if (x >= std::numeric_limits<int32_t>::min()
            and x <= std::numeric_limits<int32_t>::max()) {
          return amqp_field_value_t{
            .kind = AMQP_FIELD_KIND_I32,
            .value = {.i32 = detail::narrow<int32_t>(x)},
          };
        }
        return amqp_field_value_t{
          .kind = AMQP_FIELD_KIND_I64,
          .value = {.i64 = x},
        };
      },
      [](uint64_t x) {
        if (x
            <= detail::narrow<uint64_t>(std::numeric_limits<int32_t>::max())) {
          return amqp_field_value_t{
            .kind = AMQP_FIELD_KIND_I32,
            .value = {.i32 = detail::narrow<int32_t>(x)},
          };
        }
        return amqp_field_value_t{
          .kind = AMQP_FIELD_KIND_U64,
          .value = {.u64 = x},
        };
      },
      [](double x) {
        return amqp_field_value_t{
          .kind = AMQP_FIELD_KIND_F64,
          .value = {.f64 = x},
        };
      },
      [&](const std::string& x) {
        string_values_.push_back(x);
        return amqp_field_value_t{
          .kind = AMQP_FIELD_KIND_UTF8,
          .value = {.bytes = as_amqp_bytes(string_values_.back())},
        };
      },
      [](const auto&) -> amqp_field_value_t {
        // validated in describe()
        TENZIR_UNREACHABLE();
      });
  }

  std::vector<std::string> keys_;
  std::vector<std::string> string_values_;
  std::vector<amqp_table_entry_t> entries_;
  amqp_table_t table_{amqp_empty_table};
};

namespace legacy {

class FromAmqp final : public Operator<void, table_slice> {
public:
  explicit FromAmqp(FromAmqpArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto config = args_.plugin_config;
    auto requests = std::vector<secret_request>{};
    auto resolved_url = std::string{};
    requests.push_back(make_secret_request("url", args_.url, resolved_url, dh));
    if (args_.options) {
      const auto& loc = args_.options->source;
      for (const auto& [k, v] : args_.options->inner) {
        match(
          v,
          [&](const concepts::arithmetic auto& x) {
            set_or_fail(config, k, fmt::to_string(x), loc, dh);
          },
          [&](const std::string& x) {
            set_or_fail(config, k, x, loc, dh);
          },
          [&](const secret& x) {
            requests.push_back(secret_request{
              x, loc,
              [&config, k = std::string{k}, loc,
               &dh](const resolved_secret_value& v) -> failure_or<void> {
                TRY(auto str, v.utf8_view(k, loc, dh));
                set_or_fail(config, k, std::string{str}, loc, dh);
                return {};
              }});
          },
          [](const auto&) {
            // validated in describe()
            TENZIR_UNREACHABLE();
          });
      }
    }
    auto res = co_await ctx.resolve_secrets(std::move(requests));
    if (not res) {
      co_return;
    }
    auto pre_url_config = config;
    if (auto parsed = parse_url(config, resolved_url)) {
      config = std::move(*parsed);
    } else {
      diagnostic::error("failed to parse AMQP URL")
        .primary(args_.url.source)
        .hint("URL must adhere to the following format")
        .hint("amqp://[USERNAME[:PASSWORD]\\@]HOSTNAME[:PORT]/[VHOST]")
        .emit(dh);
      co_return;
    }
    if (args_.options) {
      for (const auto& [k, _] : args_.options->inner) {
        auto pre = pre_url_config.find(k);
        auto post = config.find(k);
        if (pre != pre_url_config.end() and post != config.end()
            and pre->second != post->second) {
          diagnostic::warning("option `{}` was overridden by the URL", k)
            .primary(args_.options->source)
            .emit(dh);
        }
      }
    }
    auto engine_exp = amqp_engine::make(std::move(config));
    if (not engine_exp) {
      diagnostic::error("failed to construct AMQP engine")
        .primary(args_.url.source)
        .note("{}", engine_exp.error())
        .emit(dh);
      co_return;
    }
    auto engine = std::make_shared<amqp_engine>(std::move(*engine_exp));
    auto channel = args_.channel
                     ? detail::narrow<uint16_t>(args_.channel->inner)
                     : default_channel;
    auto setup_ok = co_await spawn_blocking([&]() -> bool {
      if (auto err = engine->connect(); err.valid()) {
        diagnostic::error("failed to connect to AMQP server")
          .primary(args_.url.source)
          .note("{}", err)
          .emit(dh);
        return false;
      }
      if (auto err = engine->open(channel); err.valid()) {
        diagnostic::error("failed to open AMQP channel {}", channel)
          .primary(args_.url.source)
          .note("{}", err)
          .emit(dh);
        return false;
      }
      auto queue_arguments = Option<AmqpFieldTable>{};
      if (args_.queue_arguments) {
        queue_arguments.emplace(args_.queue_arguments->inner);
      }
      if (auto err = engine->start_consumer({
            .channel = channel,
            .exchange = args_.exchange ? std::string_view{args_.exchange->inner}
                                       : default_exchange,
            .routing_key = args_.routing_key
                             ? std::string_view{args_.routing_key->inner}
                             : default_routing_key,
            .queue = args_.queue ? std::string_view{args_.queue->inner}
                                 : default_queue,
            .passive = args_.passive,
            .durable = args_.durable,
            .exclusive = args_.exclusive,
            .auto_delete = not args_.no_auto_delete,
            .no_local = args_.no_local,
            .no_ack = not args_.ack,
            .queue_arguments
            = queue_arguments ? queue_arguments->view() : amqp_empty_table,
          });
          err.valid()) {
        diagnostic::error("failed to start AMQP consumer")
          .primary(args_.url.source)
          .note("{}", err)
          .emit(dh);
        return false;
      }
      return true;
    });
    if (not setup_ok) {
      co_return;
    }
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_amqp"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_amqp"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    ctx.spawn_task(consume_loop(std::move(engine), queue_));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    auto [tick, event] = co_await folly::coro::collectAnyNoDiscard(
      pusher_.wait(), queue_->dequeue());
    if (tick.hasValue()) {
      co_return PeriodicTick{};
    }
    if (event.hasValue()) {
      co_return std::move(event).value();
    }
    co_return Any{};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (result.try_as<PeriodicTick>()) {
      co_await pusher_.push(builder_.yield_ready(), push, events_read_counter_);
      co_return;
    }
    auto* event = result.try_as<FromAmqpEvent>();
    if (not event) {
      co_return;
    }
    co_await co_match(
      std::move(*event),
      [&](AmqpMessage msg) -> Task<void> {
        if (not msg.chunk) {
          co_return;
        }
        const auto bytes = msg.chunk->size();
        auto row = builder_.record();
        row.field("message").data(blob{as_bytes(msg.chunk)});
        if (bytes > 0) {
          bytes_read_counter_.add(bytes);
        }
        co_await pusher_.push(builder_.yield_ready(), push,
                              events_read_counter_);
      },
      [&](AmqpError err) -> Task<void> {
        diagnostic::error("failed to consume AMQP message")
          .primary(args_.url.source)
          .note("{}", err.message)
          .emit(ctx);
        co_return;
      });
  }

  auto finalize(Push<table_slice>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    for (auto&& slice : builder_.finish_as_table_slice()) {
      auto const rows = slice.rows();
      co_await push(std::move(slice));
      events_read_counter_.add(rows);
    }
    co_return FinalizeBehavior::done;
  }

private:
  static auto consume_loop(std::shared_ptr<amqp_engine> engine,
                           std::shared_ptr<FromAmqpQueue> queue) -> Task<void> {
    auto token = co_await folly::coro::co_current_cancellation_token;
    while (not token.isCancellationRequested()) {
      auto message = co_await spawn_blocking([&] {
        return engine->consume(consume_timeout);
      });
      if (not message) {
        co_await queue->enqueue(AmqpError{
          fmt::format("{}", message.error()),
        });
        co_return;
      }
      if (not *message) {
        // Timeout without a message — keep polling.
        continue;
      }
      co_await queue->enqueue(AmqpMessage{std::move(*message)});
    }
  }

  FromAmqpArgs args_;
  std::shared_ptr<FromAmqpQueue> queue_
    = std::make_shared<FromAmqpQueue>(message_queue_capacity);
  series_builder builder_{
    type{"tenzir.amqp", record_type{{"message", blob_type{}}}}};
  SeriesPusher pusher_;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
};

} // namespace legacy

class FromAmqp final : public Operator<void, nova::Events> {
public:
  explicit FromAmqp(FromAmqpArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // A restored pipeline receives the messages that a committed checkpoint
    // does not cover only if they stay on the server. Without `ack=true`, the
    // server forgets messages once it pushed them to us, which can happen
    // before a checkpoint while we emit them only after it. The queue must
    // also outlive our connection: a server-named queue gets a new name, and
    // an exclusive or auto-delete queue disappears with the connection. A
    // passive declaration neither validates nor reports these properties, so
    // checkpointing requires an active declaration. Stream queues need an
    // offset to resume, which we do not snapshot. Require an explicit classic
    // or quorum queue type because the broker default can be a stream.
    checkpointing_ = ctx.checkpoint_settings().is_some();
    if (checkpointing_) {
      auto requirement = Option<std::string_view>{};
      if (not args_.ack) {
        requirement = "`ack=true`";
      } else if (not args_.queue or args_.queue->inner.empty()) {
        requirement = "a named `queue`";
      } else if (args_.passive) {
        requirement = "`passive=false`";
      } else if (args_.exclusive) {
        requirement = "`exclusive=false`";
      } else if (not args_.no_auto_delete) {
        requirement = "`no_auto_delete=true`";
      } else if (not has_classic_or_quorum_queue_type(args_)) {
        requirement = "`queue_arguments` with `x-queue-type` set to `classic` "
                      "or `quorum`";
      }
      if (requirement) {
        diagnostic::error("`from_amqp` supports checkpointing only with {}",
                          *requirement)
          .primary(args_.url.source)
          .emit(ctx);
        done_ = true;
        co_return;
      }
    }
    auto config = co_await resolve_config(ctx, args_.url, args_.options,
                                          args_.plugin_config);
    if (not config) {
      done_ = true;
      co_return;
    }
    auto engine = amqp_engine::make(std::move(*config));
    if (not engine) {
      diagnostic::error("failed to construct AMQP engine")
        .primary(args_.url.source)
        .note("{}", engine.error())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto channel = args_.channel
                     ? detail::narrow<uint16_t>(args_.channel->inner)
                     : default_channel;
    auto failure = co_await spawn_blocking([&]() -> Option<diagnostic> {
      if (auto err = engine->connect(); err.valid()) {
        return diagnostic::error("failed to connect to AMQP server")
          .primary(args_.url.source)
          .note("{}", err)
          .done();
      }
      if (auto err = engine->open(channel); err.valid()) {
        return diagnostic::error("failed to open AMQP channel {}", channel)
          .primary(args_.url.source)
          .note("{}", err)
          .done();
      }
      auto queue_arguments = Option<AmqpFieldTable>{};
      if (args_.queue_arguments) {
        queue_arguments.emplace(args_.queue_arguments->inner);
      }
      if (auto err = engine->start_consumer({
            .channel = channel,
            .exchange = args_.exchange ? std::string_view{args_.exchange->inner}
                                       : default_exchange,
            .routing_key = args_.routing_key
                             ? std::string_view{args_.routing_key->inner}
                             : default_routing_key,
            .queue = args_.queue ? std::string_view{args_.queue->inner}
                                 : default_queue,
            .passive = args_.passive,
            .durable = args_.durable,
            .exclusive = args_.exclusive,
            .auto_delete = not args_.no_auto_delete,
            .no_local = args_.no_local,
            .no_ack = not args_.ack,
            .queue_arguments
            = queue_arguments ? queue_arguments->view() : amqp_empty_table,
          });
          err.valid()) {
        return diagnostic::error("failed to start AMQP consumer")
          .primary(args_.url.source)
          .note("{}", err)
          .done();
      }
      return None{};
    });
    if (failure) {
      ctx.dh().emit(std::move(*failure));
      done_ = true;
      co_return;
    }
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_amqp"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_amqp"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    consumer_ = ctx.spawn_task(consume_loop(std::move(*engine), channel, queue_,
                                            acks_, stop_source_.getToken()));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    auto [tick, event] = co_await folly::coro::collectAnyNoDiscard(
      timeout_.wait(), queue_->dequeue());
    // Prefer the event when both completed: dropping it would lose a message,
    // whereas `process_task()` re-arms the batch timeout for every message.
    if (event.hasValue()) {
      co_return std::move(event).value();
    }
    tick.throwUnlessValue();
    co_return PeriodicTick{};
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_) {
      co_return;
    }
    if (auto* event = result.try_as<Event>()) {
      handle(std::move(*event), ctx);
    }
    co_await push_ready(push);
  }

  auto stop(OpCtx&) -> Task<void> override {
    // We keep emitting the messages that the consumer loop already received
    // until it confirms that it stopped, because without `ack=true` the server
    // forgets messages once it delivered them.
    stop_source_.requestCancellation();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (not consumer_) {
      co_return FinalizeBehavior::done;
    }
    if (not done_) {
      // Drain the consumer loop through `await_task()`, which calls us again
      // once it stopped.
      stop_source_.requestCancellation();
      co_return FinalizeBehavior::continue_;
    }
    co_await flush(push);
    // Wait until the consumer loop applied the last acknowledgement, because
    // the server delivers unacknowledged messages again once we disconnect.
    // With checkpoints, only a committed checkpoint acknowledges messages.
    request_ack(args_.ack and not checkpointing_ ? emitted_tag_ : 0, true);
    co_await std::exchange(consumer_, None{})->try_join();
    // Once we are done, no `await_task()` competes for the queue, which holds
    // at most the failure of the last acknowledgement.
    while (auto event = queue_->try_dequeue()) {
      if (auto* err = try_as<AmqpError>(&*event)) {
        diagnostic::error("failed to acknowledge AMQP messages")
          .primary(args_.url.source)
          .note("{}", err->message)
          .emit(ctx);
      }
    }
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    // We support checkpoints only with `ack=true` and a queue that outlives
    // our connection, so the server delivers the messages that we did not emit
    // before the checkpoint again after a restore. There is nothing to
    // serialize.
    co_await flush(push);
    checkpoint_tag_ = emitted_tag_;
  }

  auto post_commit(OpCtx&) -> Task<void> override {
    // With checkpoints, we acknowledge only the messages that a committed
    // checkpoint covers, so that a restored pipeline receives the others again.
    if (args_.ack and checkpointing_ and consumer_) {
      request_ack(checkpoint_tag_, false);
    }
    co_return;
  }

private:
  /// Signals that the consumer loop stopped consuming after a graceful stop.
  struct ConsumerStopped {};

  /// Asks the consumer loop to acknowledge all deliveries up to `tag`. The
  /// last request also ends the loop.
  struct AckRequest {
    uint64_t tag = 0;
    bool last = false;
  };

  using Event = variant<amqp_engine::delivery, AmqpError, ConsumerStopped>;
  using Queue = folly::coro::BoundedQueue<Event>;
  /// Holds only the latest request, which subsumes the earlier ones because
  /// acknowledgements are cumulative.
  using AckMailbox = folly::coro::BoundedQueue<AckRequest>;

  /// Owns the connection, because RabbitMQ-C connections are not thread-safe.
  static auto consume_loop(amqp_engine engine, uint16_t channel,
                           Arc<Queue> queue, Arc<AckMailbox> acks,
                           folly::CancellationToken stop) -> Task<void> {
    auto acknowledged = uint64_t{0};
    auto result = co_await folly::coro::co_awaitTry(
      run_consumer(engine, channel, *queue, *acks, stop, acknowledged));
    // A downstream operator such as `head` cancels us without a graceful stop.
    // A pending request still covers messages that the next operator has.
    if (auto pending = acks->try_dequeue();
        pending and pending->tag > acknowledged) {
      std::ignore = co_await spawn_blocking([&] {
        return engine.ack(channel, pending->tag);
      });
    }
    co_yield folly::coro::co_result(std::move(result));
  }

  /// Forwards deliveries to the operator and applies its requests to
  /// acknowledge them in between. After a graceful stop, it only waits for
  /// requests until the last one.
  static auto run_consumer(amqp_engine& engine, uint16_t channel, Queue& queue,
                           AckMailbox& acks, folly::CancellationToken stop,
                           uint64_t& acknowledged) -> Task<void> {
    auto consuming = true;
    while (true) {
      co_await folly::coro::co_safe_point;
      auto request = Option<AckRequest>{};
      if (consuming) {
        if (auto pending = acks.try_dequeue()) {
          request = *pending;
        }
      } else {
        request = co_await acks.dequeue();
      }
      if (request) {
        if (request->tag > acknowledged) {
          auto err = co_await spawn_blocking([&] {
            return engine.ack(channel, request->tag);
          });
          if (err.valid()) {
            co_await queue.enqueue(AmqpError{fmt::format("{}", err)});
            co_return;
          }
          acknowledged = request->tag;
        }
        if (request->last) {
          co_return;
        }
      }
      if (not consuming) {
        continue;
      }
      if (stop.isCancellationRequested()) {
        consuming = false;
        co_await queue.enqueue(ConsumerStopped{});
        continue;
      }
      auto delivery = co_await spawn_blocking([&] {
        return engine.consume_delivery(consume_timeout);
      });
      if (not delivery) {
        co_await queue.enqueue(AmqpError{
          fmt::format("{}", delivery.error()),
        });
        co_return;
      }
      if (*delivery) {
        co_await queue.enqueue(std::move(**delivery));
      }
    }
  }

  /// Adds a delivered message to the batch, or records that the consumer loop
  /// ended.
  auto handle(Event event, diagnostic_handler& dh) -> void {
    match(
      std::move(event),
      [&](amqp_engine::delivery delivery) {
        add(std::move(delivery));
      },
      [&](AmqpError err) {
        diagnostic::error("failed to consume AMQP message")
          .primary(args_.url.source)
          .note("{}", err.message)
          .emit(dh);
        done_ = true;
      },
      [&](ConsumerStopped) {
        done_ = true;
      });
  }

  auto add(amqp_engine::delivery delivery) -> void {
    auto bytes = as_bytes(delivery.body);
    builder_.record().field("message").data(blob_view{bytes});
    bytes_read_counter_.add(bytes.size());
    // Acknowledging a tag acknowledges all earlier ones, so we must handle
    // deliveries in order.
    TENZIR_ASSERT(delivery.tag > handled_tag_);
    handled_tag_ = delivery.tag;
  }

  auto request_ack(uint64_t tag, bool last) -> void {
    if (auto previous = acks_->try_dequeue()) {
      tag = std::max(tag, previous->tag);
    }
    auto enqueued = acks_->try_enqueue(AckRequest{tag, last});
    TENZIR_ASSERT(enqueued);
  }

  auto push_ready(Push<nova::Events>& push) -> Task<void> {
    auto const rows = detail::narrow<size_t>(builder_.length());
    if (rows >= defaults::import::table_slice_size or timeout_.poll(rows)) {
      co_await flush(push);
    }
  }

  auto flush(Push<nova::Events>& push) -> Task<void> {
    if (builder_.length() == 0) {
      co_return;
    }
    auto data = std::exchange(builder_, {}).finish();
    timeout_.reset();
    auto const length = data.length();
    co_await push(nova::Events{
      std::move(data),
      nova::storage::BitMap{length, true},
      nova::Events::Meta::make_empty(length, "tenzir.amqp"),
    });
    events_read_counter_.add(detail::narrow<uint64_t>(length));
    emitted_tag_ = handled_tag_;
    // Without checkpoints, the messages are acknowledged once the next
    // operator has them.
    if (args_.ack and not checkpointing_) {
      request_ack(emitted_tag_, false);
    }
  }

  FromAmqpArgs args_;
  mutable Arc<Queue> queue_{std::in_place, message_queue_capacity};
  Arc<AckMailbox> acks_{std::in_place, uint32_t{1}};
  folly::CancellationSource stop_source_;
  Option<AsyncHandle<void>> consumer_;
  nova::ArrayBuilder<nova::Record> builder_;
  BatchTimeout timeout_{defaults::import::batch_timeout};
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  /// All deliveries up to this tag are in the batch or emitted.
  uint64_t handled_tag_ = 0;
  /// All deliveries up to this tag are emitted.
  uint64_t emitted_tag_ = 0;
  /// All deliveries up to this tag are covered by the pending checkpoint.
  uint64_t checkpoint_tag_ = 0;
  bool checkpointing_ = false;
  bool done_ = false;
};

class from_amqp_plugin final : public virtual OperatorPlugin {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `amqp.yaml` instead",
                               this->name())
        .to_error();
    }
    auto c = try_get_only<tenzir::record>(global_config, "plugins.amqp");
    if (not c) {
      return c.error();
    }
    if (*c) {
      config_ = **c;
    }
    return caf::none;
  }

  auto name() const -> std::string override {
    return "from_amqp";
  }

  auto describe() const -> Description override {
    auto args = FromAmqpArgs{};
    args.plugin_config = config_;
    auto d
      = Describer<FromAmqpArgs, legacy::FromAmqp, FromAmqp>{std::move(args)};
    d.positional("url", &FromAmqpArgs::url);
    auto channel_arg = d.named("channel", &FromAmqpArgs::channel);
    d.named("exchange", &FromAmqpArgs::exchange);
    d.named("routing_key", &FromAmqpArgs::routing_key);
    d.named("queue", &FromAmqpArgs::queue);
    auto queue_arguments_arg
      = d.named("queue_arguments", &FromAmqpArgs::queue_arguments);
    d.named("passive", &FromAmqpArgs::passive);
    d.named("durable", &FromAmqpArgs::durable);
    d.named("exclusive", &FromAmqpArgs::exclusive);
    d.named("no_auto_delete", &FromAmqpArgs::no_auto_delete);
    d.named("no_local", &FromAmqpArgs::no_local);
    d.named("ack", &FromAmqpArgs::ack);
    auto options_arg = d.named("options", &FromAmqpArgs::options);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto options = ctx.get(options_arg); options) {
        for (const auto& [k, v] : options->inner) {
          auto ok = match(
            v,
            [](const concepts::arithmetic auto&) {
              return true;
            },
            [](const concepts::one_of<std::string, secret> auto&) {
              return true;
            },
            [&](const auto&) {
              diagnostic::error(
                "expected type `number`, `bool`, `string`, or `secret` "
                "for option")
                .primary(options->source)
                .emit(ctx);
              return false;
            });
          if (not ok) {
            return {};
          }
        }
      }
      if (auto queue_arguments = ctx.get(queue_arguments_arg);
          queue_arguments) {
        for (const auto& [k, v] : queue_arguments->inner) {
          auto ok = match(
            v,
            [](const concepts::one_of<bool, int64_t, uint64_t, double,
                                      std::string> auto&) {
              return true;
            },
            [&](const auto&) {
              diagnostic::error(
                "expected type `number`, `bool`, or `string` for queue "
                "argument")
                .primary(queue_arguments->source.subloc(0, 1))
                .hint("unsupported key: `{}`", k)
                .emit(ctx);
              return false;
            });
          if (not ok) {
            return {};
          }
        }
      }
      if (auto channel = ctx.get(channel_arg); channel) {
        if (channel->inner > std::numeric_limits<uint16_t>::max()) {
          diagnostic::error("`channel` must fit into 16 bits")
            .primary(channel->source)
            .emit(ctx);
          return {};
        }
      }
      return {};
    });
    // AMQP distributes messages among concurrent consumers of classic and
    // quorum queues. Require an explicit queue type because the broker default
    // can be a stream queue, whose consumers maintain independent offsets.
    d.parallelizable([](const FromAmqpArgs& args) {
      return args.queue and not args.queue->inner.empty() and not args.exclusive
             and not args.passive and has_classic_or_quorum_queue_type(args);
    });
    return d.without_optimize();
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::amqp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::from_amqp_plugin)
