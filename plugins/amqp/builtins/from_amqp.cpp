//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/CurrentExecutor.h>

#include <limits>
#include <optional>

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
      auto queue_arguments = std::optional<AmqpFieldTable>{};
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
    return "tql2.from_amqp";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromAmqpArgs, FromAmqp>{
      FromAmqpArgs{.plugin_config = config_}};
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
    return d.without_optimize();
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::amqp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::from_amqp_plugin)
