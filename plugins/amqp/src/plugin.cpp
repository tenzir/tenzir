//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

class plugin final : public virtual loader_plugin<rabbitmq_loader>,
                     public virtual saver_plugin<rabbitmq_saver> {
public:
  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto [args, config] = parse_args<loader_args>(p);
    return std::make_unique<rabbitmq_loader>(std::move(args),
                                             std::move(config));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto [args, config] = parse_args<saver_args>(p);
    return std::make_unique<rabbitmq_saver>(std::move(args), std::move(config));
  }

  template <class Args>
  auto parse_args(parser_interface& p) const -> std::pair<Args, record> {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = Args{};
    parser.add("-c,--channel", args.channel, "<channel>");
    parser.add("-e,--exchange", args.exchange, "<exchange>");
    parser.add("-r,--routing_key", args.routing_key, "<key>");
    parser.add("-X,--set", args.options, "<key=value>,...");
    if constexpr (std::is_same_v<Args, loader_args>) {
      parser.add("-q,--queue", args.queue, "<queue>");
      parser.add("--passive", args.passive);
      parser.add("--durable", args.durable);
      parser.add("--exclusive", args.exclusive);
      parser.add("--no-auto-delete", args.no_auto_delete);
      parser.add("--no-local", args.no_local);
      parser.add("--ack", args.ack);
    } else if constexpr (std::is_same_v<Args, saver_args>) {
      parser.add("--mandatory", args.mandatory);
      parser.add("--immediate", args.immediate);
    }
    parser.add(args.url, "<url>");
    parser.parse(p);
    auto config = config_;
    if (args.url) {
      if (auto cfg = parse_url(config_, args.url->inner)) {
        config = std::move(*cfg);
      } else {
        diagnostic::error("failed to parse AMQP URL")
          .primary(args.url->source)
          .hint("URL must adhere to the following format")
          .hint("amqp://[USERNAME[:PASSWORD]\\@]HOSTNAME[:PORT]/[VHOST]")
          .throw_();
      }
    }
    if (args.options) {
      std::vector<std::pair<std::string, std::string>> kvps;
      if (not parsers::kvp_list(args.options->inner, kvps)) {
        diagnostic::error("invalid list of key=value pairs")
          .primary(args.options->source)
          .throw_();
      }
      // For all string keys, we don't attempt automatic conversion.
      auto strings = std::set<std::string>{"hostname", "vhost", "sasl_method",
                                           "username", "password"};
      for (auto& [key, value] : kvps) {
        if (strings.contains(key)) {
          config[key] = std::move(value);
        } else if (auto x = from_yaml(value)) {
          config[key] = std::move(*x);
        } else {
          diagnostic::error("failed to parse value in key-value pair")
            .primary(args.options->source)
            .note("value: {}", value)
            .throw_();
        }
      }
    }
    return {std::move(args), std::move(config)};
  }

  auto name() const -> std::string override {
    return "amqp";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::amqp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::plugin)
