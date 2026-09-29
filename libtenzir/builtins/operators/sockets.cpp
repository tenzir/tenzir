//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/os.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::sockets {

namespace {

struct SocketsArgs {
  // No arguments.
};

class Sockets final : public Operator<void, table_slice> {
public:
  explicit Sockets(SocketsArgs /*args*/) {
  }

  auto start(OpCtx&) -> Task<void> override {
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result);
    auto system = os::make();
    if (not system) {
      diagnostic::error("failed to create OS shim").emit(ctx.dh());
      done_ = true;
      co_return;
    }
    co_await push(system->sockets());
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class SocketsEvents final : public Operator<void, nova::Events> {
public:
  explicit SocketsEvents(SocketsArgs /*args*/) {
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await spawn_blocking([] -> Option<std::vector<net_socket>> {
      auto system = os::make();
      if (not system) {
        return None{};
      }
      return system->socket_data();
    });
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto sockets = std::move(result).as<Option<std::vector<net_socket>>>();
    if (not sockets) {
      diagnostic::error("failed to create OS shim").emit(ctx.dh());
      done_ = true;
      co_return;
    }
    auto builder = nova::ArrayBuilder<nova::Record>{};
    for (const auto& socket : *sockets) {
      auto event = builder.record();
      event.field("pid").data(static_cast<uint64_t>(socket.pid));
      if (socket.process_name.empty()) {
        event.field("process").null();
      } else {
        event.field("process").data(socket.process_name);
      }
      event.field("protocol").data(static_cast<uint64_t>(socket.protocol));
      event.field("local_addr").data(socket.local_addr);
      event.field("local_port").data(static_cast<uint64_t>(socket.local_port));
      event.field("remote_addr").data(socket.remote_addr);
      event.field("remote_port").data(static_cast<uint64_t>(socket.remote_port));
      if (socket.state.empty()) {
        event.field("state").null();
      } else {
        event.field("state").data(socket.state);
      }
    }
    auto output = builder.finish();
    auto rows = output.length();
    co_await push(
      nova::Events{std::move(output), nova::storage::BitMap{rows, true},
                   nova::Events::Meta::make_empty(rows, "tenzir.socket")});
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }
  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "sockets";
  }

  auto describe() const -> Description override {
    auto d = Describer<SocketsArgs, Sockets, SocketsEvents>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::sockets

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sockets::Plugin)
