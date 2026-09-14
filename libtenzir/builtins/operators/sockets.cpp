//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "sockets";
  }

  auto describe() const -> Description override {
    auto d = Describer<SocketsArgs, Sockets>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::sockets

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sockets::Plugin)
