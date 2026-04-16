//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/config.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

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

template <class Operator, class Args>
class plugin : public virtual operator_plugin2<Operator> {
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
      global_defaults() = **c;
    }
    return caf::none;
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = Args{};
    args.op = inv.self.get_location();
    auto channel = std::optional<located<uint64_t>>{};
    auto parser = argument_parser2::operator_(this->name());
    parser.positional("url", args.url);
    parser.named("channel", channel);
    parser.named("exchange", args.exchange);
    parser.named("routing_key", args.routing_key);
    parser.named("options", args.options);
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
    if (args.options) {
      for (const auto& [k, v] : args.options->inner) {
        auto result = match(
          v,
          [](const concepts::arithmetic auto&) -> failure_or<void> {
            return {};
          },
          [](const concepts::one_of<std::string, secret> auto&)
            -> failure_or<void> {
            return {};
          },
          [&](const auto&) -> failure_or<void> {
            diagnostic::error(
              "expected type `number`, `bool` or `string` for option")
              .primary(args.options->source)
              .emit(ctx);
            return failure::promise();
          });
        TRY(result);
      }
    }
    return std::make_unique<Operator>(std::move(args), std::move(config));
  }

  virtual auto load_properties() const
    -> operator_factory_plugin::load_properties_t override {
    if constexpr (std::same_as<Operator, rabbitmq_loader>) {
      return {
        .schemes = {"amqp", "amqps"},
      };
    } else {
      return operator_factory_plugin::load_properties();
    }
  }

  virtual auto save_properties() const
    -> operator_factory_plugin::save_properties_t override {
    if constexpr (std::same_as<Operator, rabbitmq_saver>) {
      return {
        .schemes = {"amqp", "amqps"},
      };
    } else {
      return operator_factory_plugin::save_properties();
    }
  }

protected:
  record config_;
};

class load_plugin final : public virtual plugin<rabbitmq_loader, loader_args>,
                          public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<from_amqp_args, FromAmqp>{};
    d.operator_location(&from_amqp_args::op);
    d.positional("url", &from_amqp_args::url);
    d.named("channel", &from_amqp_args::channel);
    d.named("exchange", &from_amqp_args::exchange);
    d.named("routing_key", &from_amqp_args::routing_key);
    d.named("options", &from_amqp_args::options);
    d.named("queue", &from_amqp_args::queue);
    d.named("passive", &from_amqp_args::passive);
    d.named("durable", &from_amqp_args::durable);
    d.named("exclusive", &from_amqp_args::exclusive);
    d.named("no_auto_delete", &from_amqp_args::no_auto_delete);
    d.named("no_local", &from_amqp_args::no_local);
    d.named("ack", &from_amqp_args::ack);
    return d.without_optimize();
  }
};

class save_plugin final : public virtual plugin<rabbitmq_saver, saver_args>,
                          public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<to_amqp_args, ToAmqp>{};
    d.operator_location(&to_amqp_args::op);
    d.positional("url", &to_amqp_args::url);
    d.named("channel", &to_amqp_args::channel);
    d.named("exchange", &to_amqp_args::exchange);
    d.named("routing_key", &to_amqp_args::routing_key);
    d.named("options", &to_amqp_args::options);
    d.named("mandatory", &to_amqp_args::mandatory);
    d.named("immediate", &to_amqp_args::immediate);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::amqp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::save_plugin)
