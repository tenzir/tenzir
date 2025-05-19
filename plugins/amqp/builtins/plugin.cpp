//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/config.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/plugin.hpp>

#include <caf/expected.hpp>

#include "operator.hpp"

#if __has_include(<rabbitmq-c/amqp.h>)
#  include <rabbitmq-c/amqp.h>
#  include <rabbitmq-c/ssl_socket.h>
#  include <rabbitmq-c/tcp_socket.h>
#else
#  include <amqp.h>
#  include <amqp_ssl_socket.h>
#  include <amqp_tcp_socket.h>
#endif

using namespace std::chrono_literals;

namespace tenzir::plugins::amqp {

namespace {

constexpr auto stringify = detail::overload{
  [](const concepts::arithmetic auto& value) -> std::optional<std::string> {
    return fmt::to_string(value);
  },
  [](std::string value) -> std::optional<std::string> {
    return value;
  },
  [](const auto&) -> std::optional<std::string> {
    return std::nullopt;
  },
};

template <template <class, detail::string_literal = ""> class Adapter,
          class Plugin, class Args>
class plugin : public virtual operator_plugin2<Adapter<Plugin>> {
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `amqp.yaml` "
                               "instead",
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

  auto make(operator_factory_plugin::invocation inv,
            session ctx) const -> failure_or<operator_ptr> override {
    auto args = Args{};
    auto channel = std::optional<located<uint64_t>>{};
    auto options = std::optional<located<record>>{};
    auto parser = argument_parser2::operator_(this->name());
    parser.positional("url", args.url);
    parser.named("channel", channel);
    parser.named("exchange", args.exchange);
    parser.named("routing_key", args.routing_key);
    parser.named("options", options);
    if constexpr (std::is_same_v<Args, loader_args>) {
      parser.named("queue", args.queue);
      parser.named("passive", args.passive);
      parser.named("durable", args.durable);
      parser.named("exclusive", args.exclusive);
      parser.named("no_auto_delete", args.no_auto_delete);
      parser.named("no_local", args.no_local);
      parser.named("ack", args.ack);
    } else if constexpr (std::is_same_v<Args, saver_args>) {
      parser.named("mandatory", args.mandatory);
      parser.named("immediate", args.immediate);
    }
    TRY(parser.parse(inv, ctx));
    auto config = config_;
    // FIXME: Add support to arg parser for other fixed-bit numeric types
    if (channel) {
      args.channel
        = {detail::narrow<uint16_t>(channel->inner), channel->source};
    }
    if (args.url) {
      if (auto cfg = parse_url(config_, args.url->inner)) {
        config = std::move(*cfg);
      } else {
        diagnostic::error("failed to parse AMQP URL")
          .primary(args.url->source)
          .hint("URL must adhere to the following format")
          .hint("amqp://[USERNAME[:PASSWORD]\\@]HOSTNAME[:PORT]/[VHOST]")
          .emit(ctx);
        return failure::promise();
      }
    }
    if (options) {
      // For all string keys, we don't attempt automatic conversion.
      const auto strings = std::set<std::string>{
        "hostname", "vhost", "sasl_method", "username", "password"};
      for (auto& [k, v] : options->inner) {
        if (auto str = match(v, stringify)) {
          if (strings.contains(k)) {
            config[k] = std::move(str).value();
          } else if (auto x = from_yaml(str.value())) {
            config[k] = std::move(*x);
          } else {
            diagnostic::error("failed to parse value in key-value pair")
              .primary(options->source)
              .note("value: {}", v)
              .emit(ctx);
            return failure::promise();
          }
          continue;
        }
        diagnostic::error(
          "expected type `number`, `bool` or `string` for option")
          .primary(options->source)
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<Adapter<Plugin>>(
      Plugin{std::move(args), std::move(config)});
  }

  virtual auto load_properties() const
    -> operator_factory_plugin::load_properties_t override {
    if constexpr (std::same_as<Plugin, rabbitmq_loader>) {
      return {
        .schemes = {"amqp", "amqps"},
      };
    } else {
      return operator_factory_plugin::load_properties();
    }
  }

  virtual auto save_properties() const
    -> operator_factory_plugin::save_properties_t override {
    if constexpr (std::same_as<Plugin, rabbitmq_saver>) {
      return {
        .schemes = {"amqp", "amqps"},
      };
    } else {
      return operator_factory_plugin::save_properties();
    }
  }

private:
  record config_;
};

using load_plugin = plugin<loader_adapter, rabbitmq_loader, loader_args>;
using save_plugin = plugin<saver_adapter, rabbitmq_saver, saver_args>;

} // namespace

} // namespace tenzir::plugins::amqp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::save_plugin)
