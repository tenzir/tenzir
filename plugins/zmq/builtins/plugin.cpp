//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

// TODO: Collapse these
class load_plugin final
  : public virtual operator_plugin2<loader_adapter<zmq_loader>> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = loader_args{};
    TRY(argument_parser2::operator_(name())
          .add(args.endpoint, "<endpoint>")
          .add("filter", args.filter)
          .add("listen", args.listen)
          .add("connect", args.connect)
          .add("monitor", args.monitor)
          .parse(inv, ctx));
    if (args.listen and args.connect) {
      diagnostic::error("`listen` and `connect` are mutually exclusive")
        .primary(*args.listen)
        .primary(*args.connect)
        .emit(ctx);
      return failure::promise();
    }
    if (not args.endpoint) {
      args.endpoint = located<std::string>{default_endpoint, location::unknown};
    } else if (args.endpoint->inner.find("://") == std::string::npos) {
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    }
    if (not args.endpoint->inner.starts_with("tcp://") and args.monitor) {
      diagnostic::error("`monitor` with incompatible scheme")
        .primary(*args.monitor)
        .note("`monitor` requires a TCP endpoint")
        .hint("switch to tcp://host:port or remove `monitor`")
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<loader_adapter<zmq_loader>>(
      zmq_loader{std::move(args)});
  }
};

class save_plugin final
  : public virtual operator_plugin2<saver_adapter<zmq_saver>> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = saver_args{};
    TRY(argument_parser2::operator_(name())
          .add(args.endpoint, "<endpoint>")
          .add("listen", args.listen)
          .add("connect", args.connect)
          .add("monitor", args.monitor)
          .parse(inv, ctx));
    if (args.listen and args.connect) {
      diagnostic::error("`listen` and `connect` are mutually exclusive")
        .primary(*args.listen)
        .primary(*args.connect)
        .emit(ctx);
      return failure::promise();
    }
    if (not args.endpoint) {
      args.endpoint = located<std::string>{default_endpoint, location::unknown};
    } else if (args.endpoint->inner.find("://") == std::string::npos) {
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    }
    if (not args.endpoint->inner.starts_with("tcp://") and args.monitor) {
      diagnostic::error("`monitor` with incompatible scheme")
        .primary(*args.monitor)
        .note("`monitor` requires a TCP endpoint")
        .hint("switch to tcp://host:port or remove `monitor`")
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<saver_adapter<zmq_saver>>(
      zmq_saver{std::move(args)});
  }

  // FIXME: override
  auto supported_uri_schemes() const -> std::vector<std::string> {
    return {"zmq", "inproc"};
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::save_plugin)
