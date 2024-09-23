//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/os.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::processes {

namespace {

class processes_operator final : public crtp_operator<processes_operator> {
public:
  processes_operator() = default;

  auto operator()(exec_ctx ctx) const -> generator<table_slice> {
    auto system = os::make();
    if (not system) {
      diagnostic::error("failed to create OS shim").emit(ctrl.diagnostics());
      co_return;
    }
    co_yield system->processes();
  }

  auto name() const -> std::string override {
    return "processes";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, processes_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.processes.processes_operator")
      .fields();
  }
};

class plugin final : public virtual operator_plugin<processes_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"processes", "https://docs.tenzir.com/"
                                               "operators/processes"};
    parser.parse(p);
    return std::make_unique<processes_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::processes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::processes::plugin)
