//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_expr_props.hpp"

#include "tenzir/concepts.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

#include <algorithm>
#include <ranges>
#include <string_view>
#include <vector>

namespace tenzir {

namespace {

struct ConstantScan {
  bool evaluable = true;
  bool has_secret = false;
};

/// Scans a constant value (from `ast::constant::kind` or `data`) in a single
/// pass for evaluator acceptance and legacy `tenzir::secret` occurrences.
auto scan_constant(auto const& value) -> ConstantScan {
  return tenzir::match(
    value,
    [](record const& x) -> ConstantScan {
      auto result = ConstantScan{};
      for (auto const& [_, field] : x) {
        auto const child = scan_constant(field);
        result.evaluable = result.evaluable and child.evaluable;
        result.has_secret = result.has_secret or child.has_secret;
      }
      return result;
    },
    [](list const& x) -> ConstantScan {
      auto result = ConstantScan{};
      for (auto const& item : x) {
        auto const child = scan_constant(item);
        result.evaluable = result.evaluable and child.evaluable;
        result.has_secret = result.has_secret or child.has_secret;
      }
      return result;
    },
    []<class T>(T const&) -> ConstantScan {
      return {
        .evaluable = not concepts::one_of<T, pattern, enumeration, map, secret>,
        .has_secret = std::same_as<T, secret>,
      };
    });
}

auto is_secret_call(ast::function_call const& call) -> bool {
  auto const& ref = call.fn.ref;
  return ref.pkg() == entity_pkg_std and ref.segments().size() == 1
         and ref.segments().front() == "secret";
}

class Analyzer {
public:
  explicit Analyzer(registry const* reg) : reg_{reg} {
  }

  auto run(ast::expression const& expr) -> ir::ExprProps {
    return visit(expr, false).props;
  }

private:
  struct NodeResult {
    ir::ExprProps props;
    /// True iff the subtree is an `ast::constant` or a spreadless
    /// `ast::record`/`ast::list` of constant literals.
    bool is_const_literal = false;
  };

  auto analyze_assignment(ast::assignment const& x, bool is_named_arg)
    -> ir::ExprProps {
    auto left = visit(x.left, false).props;
    auto result = visit(x.right, is_named_arg).props;
    // Neutralize the three LHS properties that reflect the target/parameter
    // identifier rather than value evaluation, then reuse `operator|` so any
    // future `ExprProps` field joins automatically.
    left.fallibility = Fallibility::total;
    left.reads_event = false;
    left.pure_constant = true;
    result |= left;
    result.node_count
      = ::tenzir::detail::saturating_add(result.node_count, uint32_t{1});
    if (not is_named_arg) {
      result.has_bare_assignment = true;
      result.fallibility = Fallibility::fails;
      result.evaluable = false;
      result.cost = std::max(result.cost, Cost::cheap);
    }
    return result;
  }

  auto visit(ast::expression const& expr, bool in_call_arg) -> NodeResult {
    return expr.match(
      [](ast::constant const& x) -> NodeResult {
        auto const scan = scan_constant(x.value);
        return {
          .props = {
            .fallibility
            = scan.evaluable ? Fallibility::total : Fallibility::fails,
            .cost = Cost::plain,
            .node_count = 1,
            .has_secret = scan.has_secret,
            .evaluable = scan.evaluable,
            .pure_constant = true,
          },
          .is_const_literal = true,
        };
      },
      [](ast::this_ const&) -> NodeResult {
        return {
          .props = {
            .fallibility = Fallibility::total,
            .cost = Cost::plain,
            .node_count = 1,
            .reads_event = true,
            .pure_constant = false,
          },
        };
      },
      [&](ast::root_field const& x) -> NodeResult {
        auto const bound = std::ranges::contains(bound_params_, x.id.name);
        return {
          .props = {
            .fallibility = (x.has_question_mark or bound) ? Fallibility::total
                                                          : Fallibility::warns,
            .cost = Cost::plain,
            .node_count = 1,
            .reads_event = not bound,
            .pure_constant = bound,
          },
        };
      },
      [](ast::meta const&) -> NodeResult {
        return {
          .props = {
            .fallibility = Fallibility::total,
            .cost = Cost::plain,
            .node_count = 1,
            .reads_event = true,
            .pure_constant = false,
          },
        };
      },
      [&](ast::field_access const& x) -> NodeResult {
        // Even `a.?b` warns when `a` is a non-record alternative.
        auto result = ir::ExprProps{
          .fallibility = Fallibility::warns,
          .cost = Cost::plain,
          .node_count = 1,
        };
        result |= visit(x.left, false).props;
        return {.props = result};
      },
      [&](ast::index_expr const& x) -> NodeResult {
        auto result = ir::ExprProps{
          .fallibility = Fallibility::warns,
          .cost = is<ast::constant>(x.index) ? Cost::plain : Cost::cheap,
          .node_count = 1,
        };
        result |= visit(x.expr, false).props;
        result |= visit(x.index, false).props;
        return {.props = result};
      },
      [&](ast::binary_expr const& x) -> NodeResult {
        auto result = ir::ExprProps{
          .fallibility = Fallibility::warns,
          .cost = Cost::cheap,
          .node_count = 1,
        };
        result |= visit(x.left, false).props;
        result |= visit(x.right, false).props;
        return {.props = result};
      },
      [&](ast::unary_expr const& x) -> NodeResult {
        auto result = ir::ExprProps{
          .fallibility = Fallibility::warns,
          .cost = Cost::cheap,
          .node_count = 1,
          .has_move = x.op == ast::unary_op::move,
        };
        result |= visit(x.expr, false).props;
        return {.props = result};
      },
      [&](ast::function_call const& x) -> NodeResult {
        auto result = ir::ExprProps{
          .node_count = 1,
          .has_function_call = true,
        };
        if (not x.fn.ref.resolved()) {
          result.volatility = Volatility::volatile_;
          result.fallibility = Fallibility::fails;
          result.cost = Cost::expensive;
          result.expensive_ops = 1;
          result.has_non_value = true;
          result.pure_constant = false;
        } else if (reg_ == nullptr) {
          result.volatility = Volatility::volatile_;
          result.fallibility = Fallibility::fails;
          result.cost = Cost::expensive;
          result.expensive_ops = 1;
          result.has_secret = is_secret_call(x);
          result.pure_constant = false;
        } else {
          auto const& plugin = reg_->get(x);
          auto const fn_vol = plugin.volatility();
          auto const fn_fall = plugin.fallibility();
          auto const fn_cost = plugin.cost();
          TENZIR_ASSERT(fn_cost != Cost::plain);
          result.volatility = fn_vol;
          result.fallibility = fn_fall;
          result.cost = std::max(Cost::cheap, fn_cost);
          result.expensive_ops = fn_cost == Cost::expensive ? 1u : 0u;
          result.has_secret = is_secret_call(x);
          result.pure_constant = fn_vol == Volatility::immutable;
        }
        for (auto const& arg : x.args) {
          if (auto const* named = try_as<ast::assignment>(arg)) {
            result |= analyze_assignment(*named, true);
          } else {
            result |= visit(arg, true).props;
          }
        }
        return {.props = result};
      },
      [&](ast::record const& x) -> NodeResult {
        auto result = ir::ExprProps{.node_count = 1};
        auto spreads = size_t{0};
        auto all_const_lit = true;
        for (auto const& item : x.items) {
          item.match(
            [&](ast::record::field const& field) {
              auto const child = visit(field.expr, false);
              result |= child.props;
              all_const_lit = all_const_lit and child.is_const_literal;
            },
            [&](ast::spread const& spread) {
              ++spreads;
              all_const_lit = false;
              result |= visit(spread.expr, false).props;
            });
        }
        if (spreads > 1) {
          // Multi-spread records evaluate in the columnar evaluator, so
          // `evaluable` stays true while `fallibility` stays `fails` for
          // legacy evaluator conservatism.
          result.fallibility = Fallibility::fails;
        } else if (spreads == 1) {
          result.fallibility = std::max(result.fallibility, Fallibility::warns);
        }
        auto const is_const_lit = spreads == 0 and all_const_lit;
        if (not is_const_lit) {
          result.cost = std::max(result.cost, Cost::cheap);
        }
        return {.props = result, .is_const_literal = is_const_lit};
      },
      [&](ast::list const& x) -> NodeResult {
        auto result = ir::ExprProps{.node_count = 1};
        auto has_spread = false;
        auto all_const_lit = true;
        for (auto const& item : x.items) {
          item.match(
            [&](ast::expression const& element) {
              auto const child = visit(element, false);
              result |= child.props;
              all_const_lit = all_const_lit and child.is_const_literal;
            },
            [&](ast::spread const& spread) {
              has_spread = true;
              all_const_lit = false;
              result |= visit(spread.expr, false).props;
            });
        }
        // Heterogeneous lists can warn in the legacy evaluator, and spreads
        // warn on non-list values in both executors.
        if (has_spread or x.items.size() > 1) {
          result.fallibility = std::max(result.fallibility, Fallibility::warns);
        }
        auto const is_const_lit = not has_spread and all_const_lit;
        if (not is_const_lit) {
          result.cost = std::max(result.cost, Cost::cheap);
        }
        return {.props = result, .is_const_literal = is_const_lit};
      },
      [&](ast::format_expr const& x) -> NodeResult {
        auto result = ir::ExprProps{
          .fallibility = Fallibility::warns,
          .cost = Cost::expensive,
          .expensive_ops = 1,
          .node_count = 1,
        };
        for (auto const& segment : x.segments) {
          if (auto const* rep
              = try_as<ast::format_expr::replacement>(segment)) {
            result |= visit(rep->expr, false).props;
          }
        }
        return {.props = result};
      },
      [&](ast::lambda_expr const& x) -> NodeResult {
        auto const saved = bound_params_.size();
        for (auto const& param : x.params) {
          bound_params_.push_back(param.name);
        }
        auto const body = visit(x.body, false).props;
        bound_params_.resize(saved);
        auto result = ir::ExprProps{
          .fallibility = in_call_arg ? Fallibility::total : Fallibility::fails,
          .cost = Cost::expensive,
          .expensive_ops = 1,
          .node_count = 1,
          .evaluable = in_call_arg,
        };
        result |= body;
        return {.props = result};
      },
      [&](ast::unpack const& x) -> NodeResult {
        auto result = ir::ExprProps{
          .fallibility = Fallibility::fails,
          .cost = Cost::cheap,
          .node_count = 1,
          .evaluable = false,
        };
        result |= visit(x.expr, false).props;
        return {.props = result};
      },
      [&](ast::assignment const& x) -> NodeResult {
        return {.props = analyze_assignment(x, false)};
      },
      [](ast::dollar_var const&) -> NodeResult {
        return {
          .props = {
            .fallibility = Fallibility::fails,
            .cost = Cost::cheap,
            .node_count = 1,
            .has_unbound_var = true,
            .evaluable = false,
            .pure_constant = false,
          },
        };
      },
      [](ast::pkg_dollar_var const& x) -> NodeResult {
        auto const scan
          = x.value ? scan_constant(*x.value)
                    : ConstantScan{.evaluable = false, .has_secret = false};
        return {
          .props = {
            .fallibility = Fallibility::fails,
            .cost = Cost::cheap,
            .node_count = 1,
            .has_secret = scan.has_secret,
            .evaluable = scan.evaluable,
            .pure_constant = true,
          },
        };
      },
      [](ast::pipeline_expr const&) -> NodeResult {
        return {
          .props = {
            .fallibility = Fallibility::fails,
            .cost = Cost::expensive,
            .expensive_ops = 1,
            .node_count = 1,
            .has_non_value = true,
            .evaluable = false,
            .pure_constant = false,
          },
        };
      },
      [](ast::underscore const&) -> NodeResult {
        return {
          .props = {
            .fallibility = Fallibility::fails,
            .cost = Cost::cheap,
            .node_count = 1,
            .evaluable = false,
            .pure_constant = true,
          },
        };
      },
      [](ast::type_expr const&) -> NodeResult {
        return {
          .props = {
            .fallibility = Fallibility::fails,
            .cost = Cost::cheap,
            .node_count = 1,
            .evaluable = false,
            .pure_constant = true,
          },
        };
      },
      [](ast::resolved_secret const&) -> NodeResult {
        return {
          .props = {
            .fallibility = Fallibility::fails,
            .cost = Cost::cheap,
            .node_count = 1,
            .has_secret = true,
            .evaluable = false,
            .pure_constant = false,
          },
        };
      });
  }

  registry const* reg_ = nullptr;
  std::vector<std::string_view> bound_params_;
};

} // namespace

auto ir::analyze(ast::expression const& expr, registry const& reg)
  -> ExprProps {
  return Analyzer{&reg}.run(expr);
}

auto detail::analyze_expr_without_registry(ast::expression const& expr)
  -> ir::ExprProps {
  return Analyzer{nullptr}.run(expr);
}

} // namespace tenzir
