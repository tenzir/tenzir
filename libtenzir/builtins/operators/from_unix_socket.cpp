//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/stream.hpp>
#include <tenzir/async/uds.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/file.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator/stream_from.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

namespace tenzir::plugins::from_unix_socket {

namespace {

constexpr auto connect_timeout = std::chrono::seconds{5};

struct UnixSocketFrom {
  struct Args {
    located<std::string> path;
    located<ir::pipeline> user_pipeline;
  };

  struct ConnectionInfo {};

  explicit UnixSocketFrom(Args args) : args_{std::move(args)} {
  }

  auto prepare(OpCtx& ctx) -> Task<bool> {
    path_ = expand_home(args_.path.inner);
    auto address = make_uds_socket_address(path_, args_.path.source, ctx.dh());
    if (not address) {
      co_return false;
    }
    address_ = std::move(*address);
    co_return true;
  }

  auto connect(folly::EventBase* evb) const
    -> Task<Box<folly::coro::Transport>> {
    TENZIR_DEBUG("from_unix_socket: connecting to {}", path_);
    TENZIR_ASSERT(address_);
    auto transport = co_await folly::coro::co_withExecutor(
      evb, folly::coro::Transport::newConnectedSocket(
             evb, *address_,
             std::chrono::duration_cast<std::chrono::milliseconds>(
               connect_timeout)));
    TENZIR_DEBUG("from_unix_socket: connected to {}", path_);
    co_return Box<folly::coro::Transport>{std::move(transport)};
  }

  auto make_connection_info(folly::coro::Transport const&, OpCtx&) const
    -> ConnectionInfo {
    return {};
  }

  auto substitute(ir::pipeline& pipeline, ConnectionInfo&, OpCtx& ctx) const
    -> bool {
    return static_cast<bool>(
      pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true));
  }

  auto pipeline() -> located<ir::pipeline>& {
    return args_.user_pipeline;
  }

  auto events_metric_label() const -> MetricsLabel {
    return {"operator", "from_unix_socket"};
  }

  auto bytes_metric_label(ConnectionInfo const&) const -> MetricsLabel {
    return {"operator", "from_unix_socket"};
  }

  auto ready() const -> bool {
    return static_cast<bool>(address_);
  }

  auto emit_connect_warning(folly::AsyncSocketException const& ex,
                            diagnostic_handler& dh) const -> void {
    diagnostic::warning("failed to connect to UNIX domain socket")
      .primary(args_.path.source)
      .note("path: {}", path_)
      .note("reason: {}", describe_socket_error(ex))
      .hint("ensure a server is listening on this socket path")
      .emit(dh);
  }

  auto emit_read_warning(std::string const& error, diagnostic_handler& dh) const
    -> void {
    diagnostic::warning("connection closed after read error")
      .primary(args_.path.source)
      .note("path: {}", path_)
      .note("reason: {}", error)
      .emit(dh);
  }

private:
  Args args_;
  std::string path_;
  Option<folly::SocketAddress> address_;
};

using FromUnixSocketArgs = UnixSocketFrom::Args;
using FromUnixSocket = StreamFrom<UnixSocketFrom>;

class FromUnixSocketPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_unix_socket";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromUnixSocketArgs, FromUnixSocket>{};
    d.positional("path", &FromUnixSocketArgs::path);
    auto pipeline_arg = d.pipeline(&FromUnixSocketArgs::user_pipeline,
                                   SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto pipeline, ctx.get(pipeline_arg));
      auto output = pipeline.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (output->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(pipeline.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_unix_socket

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_unix_socket::FromUnixSocketPlugin)
