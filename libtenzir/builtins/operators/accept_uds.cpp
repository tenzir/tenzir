//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/stream_accept.hpp>
#include <tenzir/async/uds.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/file.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <filesystem>
#include <limits>

namespace tenzir::plugins::accept_uds {

namespace {

constexpr auto listen_backlog = uint32_t{128};

struct UdsAccept {
  using Connection = Arc<folly::coro::Transport>;

  struct Args {
    located<std::string> path;
    Option<located<uint64_t>> max_connections;
    located<ir::pipeline> user_pipeline;
  };

  struct AcceptedInfo {
    Connection transport;
  };

  struct ConnectionState {};

  explicit UdsAccept(Args args) : args_{std::move(args)} {
  }

  auto max_connections() const -> uint64_t {
    return args_.max_connections ? args_.max_connections->inner : uint64_t{128};
  }

  auto prepare(OpCtx& ctx) -> Task<bool> {
    path_ = expand_home(args_.path.inner);
    auto address = make_uds_socket_address(path_, args_.path.source, ctx.dh());
    if (not address) {
      co_return false;
    }
    if (not prepare_uds_listen_path(path_, args_.path.source, ctx.dh())) {
      co_return false;
    }
    address_ = std::move(*address);
    co_return true;
  }

  auto start_listener(folly::EventBase* evb, OpCtx&) -> Task<bool> {
    TENZIR_ASSERT(address_);
    auto socket = folly::AsyncServerSocket::newSocket(evb);
    server_.emplace(
      std::in_place,
      Arc<folly::AsyncServerSocket>::from_non_null(std::move(socket)),
      *address_, listen_backlog);
    socket_path_to_cleanup_ = std::filesystem::path{path_};
    co_return true;
  }

  auto stop_accepting(folly::EventBase* evb) -> void {
    if (server_ and evb) {
      evb->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
        if (server_) {
          (*server_)->close();
        }
      });
    }
    cleanup();
  }

  auto cleanup() -> void {
    if (not socket_path_to_cleanup_) {
      return;
    }
    auto ec = std::error_code{};
    std::filesystem::remove(*socket_path_to_cleanup_, ec);
    socket_path_to_cleanup_ = None{};
  }

  auto accept(folly::EventBase* evb) -> Task<Box<folly::coro::Transport>> {
    TENZIR_ASSERT(server_);
    co_return co_await folly::coro::co_withExecutor(evb, (*server_)->accept());
  }

  auto finish_accept(Box<folly::coro::Transport> transport,
                     folly::CancellationToken, folly::CancellationToken,
                     folly::CancellationToken, diagnostic_handler&)
    -> Task<Option<AcceptedInfo>> {
    TENZIR_DEBUG("accept_uds: accepted connection on {}", path_);
    co_return AcceptedInfo{.transport = Connection{std::move(*transport)}};
  }

  auto substitute(ir::pipeline& pipeline, AcceptedInfo&, OpCtx& ctx) const
    -> bool {
    return static_cast<bool>(
      pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true));
  }

  auto make_connection_state(folly::coro::Transport&, AcceptedInfo&,
                             OpCtx&) const -> ConnectionState {
    return {};
  }

  static auto record_read(ConnectionState&, size_t) -> void {
  }

  static auto close_connection_state(ConnectionState&) -> void {
  }

  auto pipeline() -> located<ir::pipeline>& {
    return args_.user_pipeline;
  }

  auto events_metric_label() const -> MetricsLabel {
    return {"operator", "accept_uds"};
  }

  auto bytes_metric_label(AcceptedInfo const&) const -> MetricsLabel {
    return {"operator", "accept_uds"};
  }

  auto emit_accept_warning(folly::AsyncSocketException const& ex,
                           diagnostic_handler& dh) const -> void {
    diagnostic::warning("failed to accept incoming connection")
      .primary(args_.path.source)
      .note("path: {}", path_)
      .note("reason: {}", ex.what())
      .emit(dh);
  }

  auto emit_read_warning(uint64_t conn_id, std::string const& error,
                         diagnostic_handler& dh) const -> void {
    diagnostic::warning("connection closed after read error")
      .primary(args_.path.source)
      .note("connection id: {}", conn_id)
      .note("reason: {}", error)
      .emit(dh);
  }

  auto debug_name() const -> std::string_view {
    return "accept_uds";
  }

private:
  Args args_;
  std::string path_;
  Option<folly::SocketAddress> address_;
  Option<Box<UdsServerSocket>> server_;
  Option<std::filesystem::path> socket_path_to_cleanup_ = None{};
};

using AcceptUdsArgs = UdsAccept::Args;
using AcceptUds = StreamAccept<UdsAccept>;

class AcceptUdsPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.accept_uds";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptUdsArgs, AcceptUds>{};
    d.positional("path", &AcceptUdsArgs::path);
    auto max_connections_arg
      = d.named("max_connections", &AcceptUdsArgs::max_connections);
    auto pipeline_arg
      = d.pipeline(&AcceptUdsArgs::user_pipeline, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto max_connections = ctx.get(max_connections_arg);
          max_connections) {
        auto loc
          = ctx.get_location(max_connections_arg).value_or(location::unknown);
        if (max_connections->inner == 0) {
          diagnostic::error("max_connections must be greater than 0")
            .primary(loc)
            .emit(ctx);
        } else if (max_connections->inner > static_cast<uint64_t>(
                     std::numeric_limits<size_t>::max())) {
          diagnostic::error("max_connections is too large")
            .primary(loc)
            .note("maximum supported value: {}",
                  std::numeric_limits<size_t>::max())
            .emit(ctx);
        }
      }
      TRY(auto pipeline, ctx.get(pipeline_arg));
      auto output = pipeline.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<table_slice>()) {
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

} // namespace tenzir::plugins::accept_uds

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_uds::AcceptUdsPlugin)
