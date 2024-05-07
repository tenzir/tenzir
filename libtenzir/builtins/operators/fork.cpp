//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/exec.hpp"

#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::fork {

namespace {

using namespace tql2;

class fork_operator final : public crtp_operator<fork_operator> {
public:
  fork_operator() = default;

  fork_operator(located<pipeline> pipe, operator_location location)
    : pipe_{std::move(pipe)}, location_{location} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto fork_input = std::optional<table_slice>{table_slice{}};
    auto make_input = [&fork_input]() -> generator<table_slice> {
      while (fork_input) {
        co_yield std::exchange(*fork_input, {});
      }
    };
    auto instance = pipe_.inner.instantiate(make_input(), ctrl);
    if (not instance) {
      diagnostic::error(instance.error()).emit(ctrl.diagnostics());
      co_return;
    }
    auto fork = std::get_if<generator<std::monostate>>(&*instance);
    if (not fork) {
      diagnostic::error("expected sink")
        .primary(pipe_.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto it = fork->unsafe_current();
    for (auto&& slice : input) {
      // TENZIR_WARN("{}", slice.rows());
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      fork_input = slice;
      while (fork_input->rows() > 0 and it != fork->end()) {
        ++it;
      }
      co_yield slice;
    }
    fork_input = std::nullopt;
    while (it != fork->end()) {
      ++it;
    }
  }

  auto location() const -> operator_location override {
    return location_;
  }

  auto name() const -> std::string override {
    return "tql2.fork";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, fork_operator& x) -> bool {
    return f.object(x).fields(f.field("pipe", x.pipe_),
                              f.field("location", x.location_));
  }

private:
  located<pipeline> pipe_;
  operator_location location_;
};

class plugin final : public virtual tql2::operator_plugin<fork_operator> {
public:
  auto make_operator(invocation inv, session ctx) const
    -> operator_ptr override {
    if (inv.args.size() != 1) {
      diagnostic::error("TODO").primary(inv.self.get_location()).emit(ctx);
      return nullptr;
    }
    auto arg = std::get_if<ast::pipeline_expr>(&*inv.args[0].kind);
    if (not arg) {
      diagnostic::error("TODO").primary(inv.args[0].get_location()).emit(ctx);
      return nullptr;
    }
    auto pipe = prepare_pipeline(std::move(arg->inner), ctx);
    auto loc = operator_location::anywhere;
    for (auto& op : pipe.operators()) {
      auto op_loc = op->location();
      if (op_loc != operator_location::anywhere) {
        if (loc != operator_location::anywhere && loc != op_loc) {
          diagnostic::error("TODO: could not decide location")
            .primary(inv.self.get_location())
            .emit(ctx);
          return nullptr;
        }
        loc = op_loc;
      }
    }
    return std::make_unique<fork_operator>(
      located{std::move(pipe), arg->get_location()}, loc);
  }
};

} // namespace

} // namespace tenzir::plugins::fork

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::plugin)
