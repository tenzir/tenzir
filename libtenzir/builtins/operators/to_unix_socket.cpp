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
#include <tenzir/detail/narrow.hpp>
#include <tenzir/file.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator/stream_to.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>

namespace tenzir::plugins::to_unix_socket {

namespace {

constexpr auto connect_timeout = std::chrono::seconds{5};

struct UnixSocketTo {
  struct Args {
    located<std::string> path;
    Option<located<uint64_t>> max_retry_count;
    located<ir::pipeline> printer;
  };

  explicit UnixSocketTo(Args args) : args_{std::move(args)} {
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

  auto printer() -> located<ir::pipeline>& {
    return args_.printer;
  }

  auto max_retry_count() const -> Option<located<uint64_t>> const& {
    return args_.max_retry_count;
  }

  auto events_metric_label() const -> MetricsLabel {
    return {"operator", "to_unix_socket"};
  }

  auto bytes_metric_label(Option<folly::coro::Transport const&>) const
    -> Option<MetricsLabel> {
    return MetricsLabel{"operator", "to_unix_socket"};
  }

  auto connect(folly::EventBase* evb) -> Task<folly::coro::Transport> {
    TENZIR_DEBUG("to_unix_socket: connecting to {}", path_);
    auto transport = co_await folly::coro::co_withExecutor(
      evb, folly::coro::Transport::newConnectedSocket(
             evb, address_,
             std::chrono::duration_cast<std::chrono::milliseconds>(
               connect_timeout)));
    TENZIR_DEBUG("to_unix_socket: connected to {}", path_);
    co_return transport;
  }

  auto emit_connect_error(folly::AsyncSocketException const& ex,
                          uint32_t max_retry_count,
                          diagnostic_handler& dh) const -> void {
    diagnostic::error("failed to connect to UNIX domain socket: {}",
                      describe_socket_error(ex))
      .primary(args_.path.source)
      .note("path: {}", path_)
      .note("gave up after {} {}", max_retry_count,
            max_retry_count == 1 ? "retry" : "retries")
      .hint("ensure a server is listening on this socket path")
      .emit(dh);
  }

  auto emit_connect_warning(folly::AsyncSocketException const& ex,
                            diagnostic_handler& dh) const -> void {
    diagnostic::warning("failed to connect to UNIX domain socket: {}",
                        describe_socket_error(ex))
      .primary(args_.path.source)
      .note("path: {}", path_)
      .hint("ensure a server is listening on this socket path")
      .emit(dh);
  }

  auto emit_write_warning(folly::AsyncSocketException const& ex,
                          diagnostic_handler& dh) const -> void {
    diagnostic::warning("failed to write to UNIX domain socket")
      .primary(args_.path.source)
      .note("path: {}", path_)
      .note("reason: {}", ex.what())
      .note("retrying after reconnect")
      .emit(dh);
  }

private:
  Args args_;
  std::string path_;
  folly::SocketAddress address_;
};

using ToUnixSocketArgs = UnixSocketTo::Args;
using ToUnixSocket = StreamTo<UnixSocketTo>;

class ToUnixSocketPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_unix_socket";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToUnixSocketArgs, ToUnixSocket>{};
    d.positional("path", &ToUnixSocketArgs::path);
    auto max_retry_count_arg
      = d.named("max_retry_count", &ToUnixSocketArgs::max_retry_count);
    auto printer_arg
      = d.pipeline(&ToUnixSocketArgs::printer, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto max_retry_count = ctx.get(max_retry_count_arg)) {
        if (max_retry_count->inner > std::numeric_limits<uint32_t>::max()) {
          diagnostic::error("`max_retry_count` must be <= {}",
                            std::numeric_limits<uint32_t>::max())
            .primary(max_retry_count->source)
            .emit(ctx);
        }
      }
      TRY(auto printer, ctx.get(printer_arg));
      auto output = printer.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (output->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(printer.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.invariant_order_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::to_unix_socket

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_unix_socket::ToUnixSocketPlugin)
