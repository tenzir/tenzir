//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/uuid.hpp>

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

class plugin final : public virtual loader_plugin<zmq_loader>,
                     public virtual saver_plugin<zmq_saver> {
public:
  ~plugin() noexcept override {
    // Destroy the singleton.
    context.reset();
  }

  auto initialize(const record&, const record&) -> caf::error override {
    // Create the singleton.
    context = std::make_shared<::zmq::context_t>();
    return {};
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = loader_args{};
    parser.add(args.endpoint, "<endpoint>");
    parser.add("-f,--filter", args.filter, "<prefix>");
    parser.add("-l,--listen", args.listen);
    parser.add("-c,--connect", args.connect);
    parser.add("-m,--monitor", args.monitor);
    parser.parse(p);
    if (args.listen && args.connect) {
      diagnostic::error("both --listen and --connect provided")
        .primary(*args.listen)
        .primary(*args.connect)
        .hint("--listen and --connect are mutually exclusive")
        .throw_();
    }
    if (not args.endpoint) {
      args.endpoint = located<std::string>{default_endpoint, location::unknown};
    } else if (args.endpoint->inner.find("://") == std::string::npos) {
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    }
    if (not args.endpoint->inner.starts_with("tcp://") and args.monitor) {
      diagnostic::error("--monitor with incompatible scheme")
        .primary(*args.monitor)
        .note("--monitor requires a TCP endpoint")
        .hint("switch to tcp://host:port or remove --monitor")
        .throw_();
    }
    return std::make_unique<zmq_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = saver_args{};
    parser.add(args.endpoint, "<endpoint>");
    parser.add("-l,--listen", args.listen);
    parser.add("-c,--connect", args.connect);
    parser.add("-m,--monitor", args.monitor);
    parser.parse(p);
    if (args.listen && args.connect) {
      diagnostic::error("both --listen and --connect provided")
        .primary(*args.listen)
        .primary(*args.connect)
        .hint("--listen and --connect are mutually exclusive")
        .throw_();
    }
    if (not args.endpoint) {
      args.endpoint = located<std::string>{default_endpoint, location::unknown};
    } else if (args.endpoint->inner.find("://") == std::string::npos) {
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    }
    if (not args.endpoint->inner.starts_with("tcp://") and args.monitor) {
      diagnostic::error("--monitor with incompatible scheme")
        .primary(*args.monitor)
        .note("--monitor requires a TCP endpoint")
        .hint("switch to tcp://host:port or remove --monitor")
        .throw_();
    }
    return std::make_unique<zmq_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "zmq";
  }

  auto supported_uri_schemes() const -> std::vector<std::string> override {
    return {"zmq", "inproc"};
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::plugin)
