//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

namespace tenzir::plugins::move {
namespace {

struct move_operator final : public crtp_operator<move_operator> {
  move_operator() = default;

  move_operator(std::vector<ast::simple_selector> lhs,
                std::vector<ast::simple_selector> rhs)
    : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      constexpr auto drop = [](auto&&...) {
        return indexed_transformation::result_type{};
      };
      auto rights = std::vector<series>{};
      auto offsets = std::unordered_set<offset>{};
      for (const auto& r : rhs_) {
        match(
          resolve(r, slice.schema()),
          [&](const offset& of) {
            auto [ty, ptr] = of.get(slice);
            rights.emplace_back(std::move(ty), std::move(ptr));
            offsets.insert(of);
          },
          [&](const resolve_error& err) {
            match(
              err.reason,
              [&](const resolve_error::field_not_found&) {
                diagnostic::warning("field `{}` not found", err.ident.name)
                  .primary(err.ident)
                  .emit(dh);
                rights.emplace_back(series::null(
                  null_type{}, detail::narrow<int64_t>(slice.rows())));
              },
              [](const resolve_error::field_of_non_record&) {
                TENZIR_UNREACHABLE();
              });
          });
      }
      auto ts = std::vector<indexed_transformation>{};
      for (const auto& of : offsets) {
        ts.emplace_back(of, drop);
      }
      slice = transform_columns(slice, ts);
      for (const auto& [l, r] : detail::zip_equal(lhs_, rights)) {
        slice = assign(l, r, slice, dh);
      }
      co_yield slice;
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "move";
  }

  friend auto inspect(auto& f, move_operator& x) -> bool {
    return f.object(x).fields(f.field("lhs", x.lhs_), f.field("rhs", x.rhs_));
  }

  std::vector<ast::simple_selector> lhs_;
  std::vector<ast::simple_selector> rhs_;
};

struct move_plugin final : public operator_plugin2<move_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    const auto docs = std::invoke([]() {
      return argument_parser2::operator_("move").docs();
    });
    const auto usage_and_docs = [&](auto x) {
      return std::move(x).usage("move to=from, ...").docs(docs);
    };
    if (inv.args.empty()) {
      diagnostic::error("expected field assignment")
        .primary(inv.self.get_location())
        .compose(usage_and_docs)
        .emit(ctx);
      return failure::promise();
    }
    auto lhs = std::vector<ast::simple_selector>{};
    auto rhs = std::vector<ast::simple_selector>{};
    for (auto&& arg : inv.args) {
      auto* const assignment = try_as<ast::assignment>(arg);
      if (not assignment) {
        diagnostic::error("expected field assignment")
          .primary(arg)
          .compose(usage_and_docs)
          .emit(ctx);
        return failure::promise();
      }
      auto* const left = try_as<ast::simple_selector>(assignment->left);
      auto right = ast::simple_selector::try_from(assignment->right);
      if (not left or not right) {
        diagnostic::error("can only move fields")
          .primary(*assignment)
          .compose(usage_and_docs)
          .emit(ctx);
        return failure::promise();
      }
      if (right->path().empty()) {
        diagnostic::error("cannot move `this`")
          .primary(*right)
          .compose(usage_and_docs)
          .emit(ctx);
        return failure::promise();
      }
      lhs.emplace_back(std::move(*left));
      rhs.emplace_back(std::move(*right));
    }
    return std::make_unique<move_operator>(std::move(lhs), std::move(rhs));
  }
};

} // namespace
} // namespace tenzir::plugins::move

TENZIR_REGISTER_PLUGIN(tenzir::plugins::move::move_plugin)
