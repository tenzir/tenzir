//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

namespace tenzir::plugins::set_select {

namespace {

auto make_select_assignments(std::vector<ast::expression> args,
                             diagnostic_handler* dh)
  -> std::optional<std::vector<ast::assignment>> {
  auto assignments = std::vector<ast::assignment>{};
  assignments.reserve(1 + args.size());
  assignments.emplace_back(
    ast::field_path::try_from(ast::this_{}).value(), location::unknown,
    ast::record{location::unknown, {}, location::unknown});
  for (auto& arg : args) {
    if (auto* assignment = std::get_if<ast::assignment>(&*arg.kind)) {
      auto* selector = std::get_if<ast::field_path>(&assignment->left);
      if (not selector) {
        if (dh) {
          diagnostic::error("expected selector")
            .primary(assignment->left)
            .emit(*dh);
        }
        return std::nullopt;
      }
      assignments.push_back(std::move(*assignment));
      continue;
    }
    auto original_arg = arg;
    auto selector = ast::field_path::try_from(std::move(arg));
    if (not selector) {
      if (dh) {
        diagnostic::error("expected selector").primary(original_arg).emit(*dh);
      }
      return std::nullopt;
    }
    auto rhs = selector->inner();
    assignments.emplace_back(std::move(*selector), location::unknown,
                             std::move(rhs));
  }
  return assignments;
}

class set final : public operator_plugin2<set_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto usage = "set <path>=<expr>...";
    auto docs = "https://docs.tenzir.com/reference/operators/set";
    auto assignments = std::vector<ast::assignment>{};
    for (auto& arg : inv.args) {
      arg.match(
        [&](ast::assignment& x) {
          assignments.push_back(std::move(x));
        },
        [&](auto&) {
          diagnostic::error("expected assignment")
            .primary(arg)
            .usage(usage)
            .docs(docs)
            .emit(ctx.dh());
        });
    }
    return std::make_unique<set_operator>(std::move(assignments));
  }
};

class select final : public virtual operator_plugin2<set_operator>,
                     public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.select";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto assignments = make_select_assignments(std::move(inv.args), &ctx.dh());
    if (not assignments) {
      return failure::promise();
    }
    return std::make_unique<set_operator>(std::move(*assignments));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
    for (auto& arg : inv.args) {
      TRY(arg.bind(ctx));
    }
    auto& dh = static_cast<diagnostic_handler&>(ctx);
    auto assignments = make_select_assignments(std::move(inv.args), &dh);
    if (not assignments) {
      return failure::promise();
    }
    return make_set_ir(std::move(*assignments));
  }
};

} // namespace

} // namespace tenzir::plugins::set_select

TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::set)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::select)
