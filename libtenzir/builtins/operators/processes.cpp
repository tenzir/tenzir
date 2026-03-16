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

namespace tenzir::plugins::processes {

namespace {

auto make_processes(diagnostic_handler& dh) -> std::optional<table_slice> {
  auto system = os::make();
  if (not system) {
    diagnostic::error("failed to create OS shim").emit(dh);
    return std::nullopt;
  }
  return system->processes();
}

class processes_operator final : public crtp_operator<processes_operator> {
public:
  processes_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    if (auto result = make_processes(ctrl.diagnostics())) {
      co_yield std::move(*result);
    }
  }

  auto name() const -> std::string override {
    return "processes";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, processes_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.processes.processes_operator")
      .fields();
  }
};

struct ProcessesArgs {
  // No arguments.
};

class Processes final : public Operator<void, table_slice> {
public:
  explicit Processes(ProcessesArgs /*args*/) {
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
    if (auto output = make_processes(ctx.dh())) {
      co_await push(std::move(*output));
    }
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

class Plugin final : public virtual operator_plugin<processes_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_("processes").parse(inv, ctx));
    return std::make_unique<processes_operator>();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"processes", "https://docs.tenzir.com/"
                                               "operators/processes"};
    parser.parse(p);
    return std::make_unique<processes_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<ProcessesArgs, Processes>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::processes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::processes::Plugin)
