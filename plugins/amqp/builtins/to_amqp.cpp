//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/concept/printable/tenzir/json2.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>

#include <mutex>

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::amqp {

namespace {

constexpr auto message_queue_capacity = uint32_t{1024};

/// Returns the heartbeat check interval based on the negotiated heartbeat.
/// Returns 0 if heartbeats are disabled.
auto heartbeat_interval(const record& config) -> std::chrono::seconds {
  if (const auto* hb = get_if<uint64_t>(&config, "heartbeat"); hb and *hb > 0) {
    return std::chrono::seconds{std::max<uint64_t>(1, *hb / 3)};
  }
  return std::chrono::seconds{0};
}

/// Builds the default `message=` expression used by `to_amqp`.
auto default_message_expression() -> ast::expression {
  auto function
    = ast::entity{{ast::identifier{"print_ndjson", location::unknown}}};
  function.ref
    = entity_path{std::string{entity_pkg_std}, {"print_ndjson"}, entity_ns::fn};
  return ast::function_call{
    std::move(function),
    {ast::this_{location::unknown}},
    location::unknown,
    true,
  };
}

struct ToAmqpArgs {
  located<secret> url;
  ast::expression message = default_message_expression();
  Option<located<uint64_t>> channel;
  Option<located<std::string>> exchange;
  Option<located<std::string>> routing_key;
  Option<located<record>> options;
  bool mandatory = false;
  bool immediate = false;
  record plugin_config;
};

class ToAmqp final : public Operator<table_slice, void> {
public:
  explicit ToAmqp(ToAmqpArgs args)
    : args_{std::move(args)},
      is_default_printer_{compute_is_default_printer()} {
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
    auto hb_interval = heartbeat_interval(config);
    auto engine_exp = amqp_engine::make(std::move(config));
    if (not engine_exp) {
      diagnostic::error("failed to construct AMQP engine")
        .primary(args_.url.source)
        .note("{}", engine_exp.error())
        .emit(dh);
      co_return;
    }
    engine_ = std::make_shared<amqp_engine>(std::move(*engine_exp));
    channel_ = args_.channel ? detail::narrow<uint16_t>(args_.channel->inner)
                             : default_channel;
    auto setup_ok = co_await spawn_blocking([&]() -> bool {
      if (auto err = engine_->connect(); err.valid()) {
        diagnostic::error("failed to connect to AMQP server")
          .primary(args_.url.source)
          .note("{}", err)
          .emit(dh);
        return false;
      }
      if (auto err = engine_->open(channel_); err.valid()) {
        diagnostic::error("failed to open AMQP channel {}", channel_)
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
    printer_ = json_printer2{json_printer_options{
      .style = no_style(),
      .oneline = true,
    }};
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_amqp"},
                         MetricsDirection::write, MetricsVisibility::external_);
    worker_handle_
      = ctx.spawn_task(publish_loop(engine_, engine_mutex_, queue_, args_,
                                    channel_, dh, bytes_write_counter_));
    if (hb_interval > std::chrono::seconds{0}) {
      ctx.spawn_task(heartbeat_loop(engine_, engine_mutex_, hb_interval,
                                    args_.url.source, dh));
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (is_default_printer_) {
      auto b = series_builder{type{blob_type{}}};
      for (auto row : values3(input)) {
        printer_->load_new(row);
        auto bytes = printer_->bytes();
        b.data(blob{bytes});
      }
      co_await queue_->enqueue(b.finish_assert_one_array());
      co_return;
    }
    const auto& messages = eval(args_.message, input, ctx.dh());
    for (const auto& s : messages) {
      if (not s.type.kind().is<string_type>()
          and not s.type.kind().is<blob_type>()) {
        diagnostic::warning("expected `string` or `blob`, got `{}`",
                            s.type.kind())
          .primary(args_.message)
          .emit(ctx);
        continue;
      }
      co_await queue_->enqueue(s);
    }
  }

  auto finalize(OpCtx&) -> Task<FinalizeBehavior> override {
    co_await queue_->enqueue(Option<series>{});
    co_await worker_handle_->join();
    co_return FinalizeBehavior::done;
  }

private:
  using MessageQueue = folly::coro::BoundedQueue<Option<series>>;

  static auto publish_loop(std::shared_ptr<amqp_engine> engine,
                           std::shared_ptr<std::mutex> engine_mutex,
                           std::shared_ptr<MessageQueue> queue, ToAmqpArgs args,
                           uint16_t channel, diagnostic_handler& dh,
                           MetricsCounter bytes_write_counter) -> Task<void> {
    auto opts = amqp_engine::publish_options{
      .channel = channel,
      .exchange = args.exchange ? std::string_view{args.exchange->inner}
                                : default_exchange,
      .routing_key = args.routing_key
                       ? std::string_view{args.routing_key->inner}
                       : default_routing_key,
      .mandatory = args.mandatory,
      .immediate = args.immediate,
    };
    while (true) {
      auto item = co_await queue->dequeue();
      if (not item) {
        // End-of-stream sentinel.
        co_return;
      }
      auto publish_rows = [&](const auto& array) -> Task<void> {
        for (auto row = int64_t{0}; row < array.length(); ++row) {
          if (array.IsNull(row)) {
            diagnostic::warning("expected `string` or `blob`, got `null`")
              .primary(args.message)
              .emit(dh);
            continue;
          }
          auto bytes = as_bytes(array.Value(row));
          auto err = co_await spawn_blocking([&] {
            auto lock = std::lock_guard{*engine_mutex};
            return engine->publish(bytes, opts);
          });
          if (err.valid()) {
            diagnostic::error("failed to publish AMQP message")
              .primary(args.url.source)
              .note("size: {}", bytes.size())
              .note("channel: {}", opts.channel)
              .note("exchange: {}", opts.exchange)
              .note("routing key: {}", opts.routing_key)
              .note("{}", err)
              .emit(dh);
            continue;
          }
          if (not bytes.empty()) {
            bytes_write_counter.add(bytes.size());
          }
        }
      };
      if (auto strings = item->template as<string_type>()) {
        co_await publish_rows(*strings->array);
        continue;
      }
      if (auto blobs = item->template as<blob_type>()) {
        co_await publish_rows(*blobs->array);
      }
    }
  }

  static auto heartbeat_loop(std::shared_ptr<amqp_engine> engine,
                             std::shared_ptr<std::mutex> engine_mutex,
                             std::chrono::seconds interval, location loc,
                             diagnostic_handler& dh) -> Task<void> {
    while (true) {
      co_await sleep_for(interval);
      auto err = co_await spawn_blocking([&] {
        auto lock = std::lock_guard{*engine_mutex};
        return engine->process_heartbeat();
      });
      if (err.valid()) {
        diagnostic::warning("AMQP heartbeat failed")
          .primary(loc)
          .note("{}", err)
          .emit(dh);
        co_return;
      }
    }
  }

  auto compute_is_default_printer() const -> bool {
    const auto* call = try_as<ast::function_call>(args_.message);
    if (not call or not call->fn.ref.resolved()) {
      return false;
    }
    const auto& segments = call->fn.ref.segments();
    if (segments.size() != 1 or segments.front() != "print_ndjson") {
      return false;
    }
    return call->args.size() == 1 and is<ast::this_>(call->args.front());
  }

  ToAmqpArgs args_;
  std::shared_ptr<amqp_engine> engine_;
  std::shared_ptr<std::mutex> engine_mutex_ = std::make_shared<std::mutex>();
  uint16_t channel_ = default_channel;
  std::shared_ptr<MessageQueue> queue_
    = std::make_shared<MessageQueue>(message_queue_capacity);
  Option<AsyncHandle<void>> worker_handle_;
  Option<json_printer2> printer_;
  bool is_default_printer_ = false;
  MetricsCounter bytes_write_counter_;
};

class to_amqp_plugin final : public virtual OperatorPlugin {
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
    return "tql2.to_amqp";
  }

  auto describe() const -> Description override {
    auto d
      = Describer<ToAmqpArgs, ToAmqp>{ToAmqpArgs{.plugin_config = config_}};
    d.positional("url", &ToAmqpArgs::url);
    d.named_optional("message", &ToAmqpArgs::message, "blob|string");
    auto channel_arg = d.named("channel", &ToAmqpArgs::channel);
    d.named("exchange", &ToAmqpArgs::exchange);
    d.named("routing_key", &ToAmqpArgs::routing_key);
    d.named("mandatory", &ToAmqpArgs::mandatory);
    d.named("immediate", &ToAmqpArgs::immediate);
    auto options_arg = d.named("options", &ToAmqpArgs::options);
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::to_amqp_plugin)
