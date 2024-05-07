//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/check_type.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

namespace tenzir::plugins::set {

namespace {

using namespace tql2;

class plugin final : public tql2::operator_plugin<set_operator> {
public:
  auto make_operator(invocation inv, session ctx) const
    -> operator_ptr override {
    auto usage = "set <path>=<expr>...";
    auto docs = "https://docs.tenzir.com/operators/set";
    auto assignments = std::vector<ast::assignment>{};
    for (auto& arg : inv.args) {
      arg.match(
        [&](ast::assignment& x) {
          check_assignment(x, ctx);
          assignments.push_back(std::move(x));
        },
        [&](auto&) {
          diagnostic::error("expected assignment")
            .primary(arg.get_location())
            .usage(usage)
            .docs(docs)
            .emit(ctx.dh());
        });
    }
    return std::make_unique<set_operator>(std::move(assignments));
  }
};

} // namespace

} // namespace tenzir::plugins::set

TENZIR_REGISTER_PLUGIN(tenzir::plugins::set::plugin)
