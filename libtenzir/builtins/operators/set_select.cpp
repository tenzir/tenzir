//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

namespace tenzir::plugins::set_select {

namespace {

class set final : public operator_plugin2<set_operator> {
public:
  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto usage = "set <path>=<expr>...";
    auto docs = "https://docs.tenzir.com/operators/set";
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

class select final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.select";
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto assignments = std::vector<ast::assignment>{};
    assignments.reserve(1 + inv.args.size());
    assignments.emplace_back(
      ast::simple_selector::try_from(ast::this_{}).value(), location::unknown,
      ast::record{location::unknown, {}, location::unknown});
    for (auto& arg : inv.args) {
      if (auto assignment = std::get_if<ast::assignment>(&*arg.kind)) {
        auto selector = std::get_if<ast::simple_selector>(&assignment->left);
        if (not selector) {
          diagnostic::error("expected selector")
            .primary(assignment->left)
            .emit(ctx);
          continue;
        }
        assignments.push_back(std::move(*assignment));
      } else {
        auto selector = ast::simple_selector::try_from(arg);
        if (not selector) {
          diagnostic::error("expected selector").primary(arg).emit(ctx);
          continue;
        }
        // TODO: This is a hack.
        assignments.emplace_back(std::move(*selector), location::unknown,
                                 std::move(arg));
      }
    }
    return std::make_unique<set_operator>(std::move(assignments));
  }
};

} // namespace

} // namespace tenzir::plugins::set_select

TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::set)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::select)
