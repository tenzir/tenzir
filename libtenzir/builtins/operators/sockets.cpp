//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/os.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::sockets {

namespace {

class sockets_operator final : public crtp_operator<sockets_operator> {
public:
  sockets_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto system = os::make();
    if (not system) {
      diagnostic::error("failed to create OS shim").emit(ctrl.diagnostics());
      co_return;
    }
    co_yield system->sockets();
  }

  auto name() const -> std::string override {
    return "sockets";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, sockets_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sockets.sockets_operator")
      .fields();
  }
};

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
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class Plugin final : public virtual operator_plugin<sockets_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_("sockets").parse(inv, ctx));
    return std::make_unique<sockets_operator>();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"sockets", "https://docs.tenzir.com/"
                                             "operators/sockets"};
    parser.parse(p);
    return std::make_unique<sockets_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<SocketsArgs, Sockets>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::sockets

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sockets::Plugin)
