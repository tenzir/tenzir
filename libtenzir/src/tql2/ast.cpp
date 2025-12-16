//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/ast.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/try.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/detail/type_list.hpp>

#include <type_traits>
#include <unordered_set>

namespace tenzir::ast {

namespace {

class named_expression_substituter
  : public ast::visitor<named_expression_substituter> {
public:
  named_expression_substituter(
    const std::unordered_map<std::string, ast::expression>& replacements,
    diagnostic_handler& dh)
    : replacements_{replacements}, dh_{dh} {
  }

  void visit(ast::expression& expr) {
    if (auto* var = try_as<ast::dollar_var>(expr)) {
      auto name = std::string{var->name_without_dollar()};
      if (not shadowed_.contains(name)) {
        if (auto it = replacements_.find(name); it != replacements_.end()) {
          expr = it->second;
          return;
        }
      }
    }
    enter(expr);
  }

  void visit(ast::let_stmt& stmt) {
    visit(stmt.expr);
    auto name = std::string{stmt.name_without_dollar()};
    if (replacements_.contains(name)) {
      shadowed_.insert(name);
    }
  }

  void visit(ast::pipeline& pipe) {
    for (auto& stmt : pipe.body) {
      visit(stmt);
    }
  }

  void visit(ast::selector& selector) {
    selector.match(
      [&](ast::dollar_var& var) {
        auto name = std::string{var.name_without_dollar()};
        if (shadowed_.contains(name)) {
          return;
        }
        auto it = replacements_.find(name);
        if (it == replacements_.end()) {
          return;
        }
        if (auto new_selector = ast::selector::try_from(it->second)) {
          selector = std::move(*new_selector);
          return;
        }
        diagnostic::error("cannot assign to `{}` constant value", var.id.name)
          .primary(var)
          .emit(dh_);
        failure_ = failure::promise();
      },
      [](auto&) {
        // Nothing to do
      });
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto has_failed() const -> bool {
    return failure_.is_error();
  }

private:
  const std::unordered_map<std::string, ast::expression>& replacements_;
  std::unordered_set<std::string> shadowed_;
  failure_or<void> failure_;
  diagnostic_handler& dh_;
};

} // namespace

auto substitute_named_expressions(
  pipeline pipe,
  const std::unordered_map<std::string, ast::expression>& replacements,
  diagnostic_handler& dh) -> failure_or<ast::pipeline> {
  if (replacements.empty()) {
    return pipe;
  }
  auto substituter = named_expression_substituter{replacements, dh};
  substituter.visit(pipe);
  if (substituter.has_failed()) {
    return failure::promise();
  }
  return pipe;
}

auto field_path::try_from(ast::expression expr) -> std::optional<field_path> {
  // Path is collect in reversed order (outside-in).
  auto has_this = false;
  auto path = std::vector<field_path::segment>{};
  auto* current = &expr;
  while (true) {
    auto sub_result = current->match(
      [&](ast::this_&) -> variant<ast::expression*, bool> {
        has_this = true;
        return true;
      },
      [&](ast::root_field& x) -> variant<ast::expression*, bool> {
        path.emplace_back(x.id, x.has_question_mark);
        return true;
      },
      [&](ast::field_access& e) -> variant<ast::expression*, bool> {
        path.emplace_back(e.name, e.has_question_mark);
        return &e.left;
      },
      [&](ast::index_expr& e) -> variant<ast::expression*, bool> {
        auto* constant = std::get_if<ast::constant>(&*e.index.kind);
        if (not constant) {
          return false;
        }
        if (auto* name = std::get_if<std::string>(&constant->value)) {
          path.emplace_back(ast::identifier{*name, constant->source},
                            e.has_question_mark);
          return &e.expr;
        }
        return false;
      },
      [](auto&) -> variant<ast::expression*, bool> {
        return false;
      });
    if (auto* success = std::get_if<bool>(&sub_result)) {
      if (not *success) {
        return {};
      }
      std::ranges::reverse(path);
      return field_path{
        std::move(expr),
        has_this,
        std::move(path),
      };
    }
    current = std::get<ast::expression*>(sub_result);
  }
}

auto selector::try_from(ast::expression expr) -> std::optional<selector> {
  return expr.match(
    [](ast::meta& x) -> std::optional<selector> {
      return selector{x};
    },
    [&](ast::dollar_var& x) -> std::optional<selector> {
      return selector{std::move(x)};
    },
    [&](auto&) -> std::optional<selector> {
      return field_path::try_from(std::move(expr));
    });
}

auto expression::get_location() const -> location {
  return match([](const auto& x) {
    return x.get_location();
  });
}

expression::expression(expression const& other) {
  static_assert(detail::tl_empty<detail::tl_filter_not_t<
                  expression_kinds, std::is_copy_constructible>>::value);
  if (other.kind) {
    kind = std::make_unique<expression_kind>(*other.kind);
  } else {
    kind = nullptr;
  }
}

auto expression::operator=(expression const& other) -> expression& {
  if (this != &other) {
    *this = expression{other};
  }
  return *this;
}

} // namespace tenzir::ast

namespace tenzir {

namespace {

auto to_field_extractor(const ast::expression& x)
  -> std::optional<field_extractor> {
  auto p = (parsers::alpha | '_') >> *(parsers::alnum | '_');
  return x.match(
    [&](const ast::root_field& x) -> std::optional<field_extractor> {
      if (not p(x.id.name)) {
        return std::nullopt;
      }
      return x.id.name;
    },
    [&](const ast::field_access& x) -> std::optional<field_extractor> {
      if (not p(x.name.name)) {
        return std::nullopt;
      }
      if (std::holds_alternative<ast::this_>(*x.left.kind)) {
        return x.name.name;
      }
      TRY(auto left, to_field_extractor(x.left));
      return std::move(left.field) + "." + x.name.name;
    },
    [&](const ast::index_expr& x) -> std::optional<field_extractor> {
      // Legacy expressions do not differentiate between `this["a.b"]` and
      // `a.b`. We are thus slightly changing the semantics, but this seems fine
      // considering the current alternative of not optimizing such queries.
      auto index = try_as<ast::constant>(x.index);
      if (not index) {
        return std::nullopt;
      }
      auto string = try_as<std::string>(index->value);
      if (not string) {
        return std::nullopt;
      }
      if (is<ast::this_>(x.expr)) {
        return field_extractor{*string};
      }
      TRY(auto expr, to_field_extractor(x.expr));
      return fmt::format("{}.{}", expr.field, *string);
    },
    [](const auto&) -> std::optional<field_extractor> {
      return std::nullopt;
    });
}

auto to_operand(const ast::expression& x) -> std::optional<operand> {
  return x.match<std::optional<operand>>(
    [](const ast::constant& x) {
      return x.as_data();
    },
    [](const ast::list& x) -> std::optional<operand> {
      auto l = list{};
      for (const auto& item : x.items) {
        const auto* i = try_as<ast::expression>(item);
        if (not i or not is<ast::constant>(*i)) {
          return std::nullopt;
        }
        l.push_back(as<ast::constant>(*i).as_data());
      }
      return l;
    },
    [](const ast::meta& x) -> meta_extractor {
      switch (x.kind) {
        case ast::meta::name:
          return meta_extractor::schema;
        case ast::meta_kind::import_time:
          return meta_extractor::import_time;
        case ast::meta_kind::internal:
          return meta_extractor::internal;
      }
      TENZIR_UNREACHABLE();
    },
    [](const ast::function_call& x) -> std::optional<operand> {
      // TODO: Make this better.
      if (x.fn.path.size() == 1 && x.fn.path[0].name == "type_id"
          && x.args.size() == 1
          && std::holds_alternative<ast::this_>(*x.args[0].kind)) {
        return meta_extractor{meta_extractor::kind::schema_id};
      }
      return std::nullopt;
    },
    [&](const auto&) -> std::optional<operand> {
      TRY(auto field, to_field_extractor(x));
      return operand{field};
    });
}

auto to_duration_comparable(const ast::expression& e)
  -> std::optional<operand> {
  if (auto field = to_field_extractor(e)) {
    return std::move(field).value();
  }
  const auto* itime = std::get_if<ast::meta>(e.kind.get());
  if (itime and itime->kind == ast::meta::import_time) {
    return meta_extractor{meta_extractor::kind::import_time};
  }
  return std::nullopt;
}

auto fold_now(const ast::expression& l, const ast::binary_op& op,
              const ast::expression& r) -> std::optional<operand> {
  // TODO: Evaluate unary_expr to a constant duration
  auto* const constant = std::get_if<ast::constant>(r.kind.get());
  if (not constant) {
    return std::nullopt;
  }
  auto* const y = std::get_if<duration>(&constant->value);
  if (not y) {
    return std::nullopt;
  }
  auto* const call = std::get_if<ast::function_call>(l.kind.get());
  if (not call or call->fn.path[0].name != "now") {
    return std::nullopt;
  }
  if (op == ast::binary_op::add) {
    return operand{data{time::clock::now() + *y}};
  }
  TENZIR_ASSERT(op == ast::binary_op::sub);
  return operand{data{time::clock::now() - *y}};
}

// @brief Optimize x > now() +- $y <=> x > $now +- $y and x > now() +- $y
auto optimize_now(const ast::expression& left, const relational_operator& rop,
                  const ast::expression& right) -> std::optional<expression> {
  switch (rop) {
    using ro = relational_operator;
    case ro::greater:
    case ro::greater_equal:
    case ro::less:
    case ro::less_equal:
      break;
    default:
      TENZIR_UNREACHABLE();
  }
  auto field = to_duration_comparable(left);
  if (not field) {
    return std::nullopt;
  }
  auto* const bexpr = std::get_if<ast::binary_expr>(right.kind.get());
  if (not bexpr) {
    return std::nullopt;
  }
  const auto op = bexpr->op.inner;
  if (op != ast::binary_op::add and op != ast::binary_op::sub) {
    return std::nullopt;
  }
  if (auto result = fold_now(bexpr->left, op, bexpr->right)) {
    return expression{predicate{std::move(*field), rop, std::move(*result)}};
  }
  if (op == ast::binary_op::add) {
    if (auto result = fold_now(bexpr->right, op, bexpr->left)) {
      return expression{predicate{std::move(*field), rop, std::move(*result)}};
    }
  }
  return std::nullopt;
}

} // namespace

auto is_true_literal(const ast::expression& y) -> bool {
  if (auto* constant = std::get_if<ast::constant>(&*y.kind)) {
    return constant->as_data() == true;
  }
  return false;
}

auto split_legacy_expression(const ast::expression& x)
  -> std::pair<expression, ast::expression> {
  return x.match<std::pair<expression, ast::expression>>(
    [&](const ast::binary_expr& y) {
      auto rel_op = std::invoke([&]() -> std::optional<relational_operator> {
        switch (y.op.inner) {
          case ast::binary_op::add:
          case ast::binary_op::sub:
          case ast::binary_op::mul:
          case ast::binary_op::div:
          case ast::binary_op::if_:
          case ast::binary_op::else_:
            return {};
          case ast::binary_op::eq:
            return relational_operator::equal;
          case ast::binary_op::neq:
            return relational_operator::not_equal;
          case ast::binary_op::gt:
            return relational_operator::greater;
          case ast::binary_op::geq:
            return relational_operator::greater_equal;
          case ast::binary_op::lt:
            return relational_operator::less;
          case ast::binary_op::leq:
            return relational_operator::less_equal;
          case ast::binary_op::and_:
          case ast::binary_op::or_:
            return {};
          case ast::binary_op::in:
            return relational_operator::in;
        };
        TENZIR_UNREACHABLE();
      });
      constexpr auto flip_op = [](const relational_operator& op) {
        switch (op) {
          case relational_operator::less:
            return relational_operator::greater;
          case relational_operator::less_equal:
            return relational_operator::greater_equal;
          default:
            TENZIR_UNREACHABLE();
        }
      };
      if (rel_op) {
        switch (y.op.inner) {
          case ast::binary_op::gt:
          case ast::binary_op::geq:
            if (auto expr = optimize_now(y.left, *rel_op, y.right)) {
              return std::pair{std::move(expr).value(), x};
            }
            break;
          case ast::binary_op::lt:
          case ast::binary_op::leq:
            if (auto expr = optimize_now(y.right, flip_op(*rel_op), y.left)) {
              return std::pair{std::move(expr).value(), x};
            }
            break;
          default:
            break;
        }
        auto left = to_operand(y.left);
        auto right = to_operand(y.right);
        if (not left || not right) {
          return std::pair{trivially_true_expression(), x};
        }
        auto result
          = expression{predicate{std::move(*left), *rel_op, std::move(*right)}};
        if (not normalize_and_validate(result)) {
          return std::pair{trivially_true_expression(), x};
        }
        return std::pair{
          std::move(result),
          ast::expression{ast::constant{true, location::unknown}},
        };
      }
      if (y.op.inner == ast::binary_op::and_) {
        auto [lo, ln] = split_legacy_expression(y.left);
        auto [ro, rn] = split_legacy_expression(y.right);
        auto o = expression{};
        if (lo == trivially_true_expression()) {
          o = std::move(ro);
        } else if (ro == trivially_true_expression()) {
          o = std::move(lo);
        } else {
          o = conjunction{std::move(lo), std::move(ro)};
        }
        auto n = ast::expression{};
        if (is_true_literal(ln)) {
          n = std::move(rn);
        } else if (is_true_literal(rn)) {
          n = std::move(ln);
        } else {
          n = ast::expression{
            ast::binary_expr{std::move(ln), y.op, std::move(rn)}};
        }
        return std::pair{std::move(o), std::move(n)};
      }
      if (y.op.inner == ast::binary_op::or_) {
        auto [lo, ln] = split_legacy_expression(y.left);
        auto [ro, rn] = split_legacy_expression(y.right);
        // We have `(lo and ln) or (ro and rn)`, but we cannot easily split this
        // into an expression of the form `O and N`. But if `ln` and `rn` are
        // `true`, then this is just `lo or ro <=> (lo or ro) and true`.
        if (is_true_literal(ln) && is_true_literal(rn)) {
          return std::pair{expression{disjunction{lo, ro}}, std::move(ln)};
        }
      }
      return std::pair{trivially_true_expression(), x};
    },
    [&](const ast::unary_expr& y) {
      if (y.op.inner == ast::unary_op::not_) {
        auto split = split_legacy_expression(y.expr);
        // TODO: When exactly can we split this?
        if (is_true_literal(split.second)) {
          // There's a bug in the parsing of TQL1 expressions that effectively
          // forbids nested negations. Because we roundtrip TQL1 expressions as
          // part of our predicate pushdown, we need to handle this case
          // specially.
          if (auto* neg_first = try_as<negation>(split.first)) {
            return std::pair{expression{neg_first->expr()}, split.second};
          }
          return std::pair{expression{negation{split.first}}, split.second};
        }
      }
      return std::pair{trivially_true_expression(), x};
    },
    [&](const auto&) {
      if (auto field = to_field_extractor(x)) {
        return std::pair{
          expression{predicate{*field, relational_operator::equal, data{true}}},
          ast::expression{ast::constant{true, location::unknown}},
        };
      }
      return std::pair{trivially_true_expression(), x};
    });
}

namespace {

class binder : public ast::visitor<binder> {
public:
  explicit binder(compile_ctx ctx) : ctx_{ctx} {
  }

  void visit(ast::dollar_var& x) {
    if (x.let) {
      return;
    }
    auto name = x.name_without_dollar();
    if (auto let = ctx_.get(name)) {
      x.let = *let;
    } else {
      auto available = std::vector<std::string>{};
      for (auto& [name, _] : ctx_.env()) {
        available.push_back("`$" + name + "`");
      }
      auto d = diagnostic::error("unknown variable").primary(x);
      if (available.empty()) {
        d = std::move(d).hint("no variables are available here");
      } else {
        d = std::move(d).hint("available are {}", fmt::join(available, ", "));
      }
      std::move(d).emit(ctx_);
      result_ = failure::promise();
    }
  }

  void visit(ast::pipeline_expr& x) {
    // TODO: What should happen? If `ast::constant` would allow `ir::pipeline`,
    // then we could compile it here. However, what environment do we take?
    diagnostic::error("cannot have pipeline here").primary(x.begin).emit(ctx_);
    result_ = failure::promise();
    // enter(x);
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto result() -> failure_or<void> {
    return result_;
  }

private:
  failure_or<void> result_;
  compile_ctx ctx_;
};

} // namespace

auto ast::expression::bind(compile_ctx ctx) & -> failure_or<void> {
  auto b = binder{ctx};
  b.visit(*this);
  return b.result();
}

namespace {

class substitutor : public ast::visitor<substitutor> {
public:
  explicit substitutor(substitute_ctx ctx) : ctx_{ctx} {
  }

  void visit(ast::expression& x) {
    if (auto* var = try_as<ast::dollar_var>(x)) {
      if (auto value = ctx_.get(var->let)) {
        x = ast::constant{std::move(*value), var->get_location()};
      } else {
        result_ = ast::substitute_result::some_remaining;
      }
    } else {
      enter(x);
    }
  }

  static void visit(ast::dollar_var&) {
    // This is handled by the `ast::expression` case above.
    TENZIR_UNREACHABLE();
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto result() const -> ast::substitute_result {
    return result_;
  }

private:
  ast::substitute_result result_ = ast::substitute_result::no_remaining;
  substitute_ctx ctx_;
};

} // namespace

// TODO: Where to put this?
auto ast::expression::substitute(
  substitute_ctx ctx) & -> failure_or<substitute_result> {
  auto visitor = substitutor{ctx};
  visitor.visit(*this);
  return visitor.result();
}

auto ast::expression::is_deterministic(const registry& reg) const -> bool {
  // TODO: Handle other cases.
  return match(
    [](const ast::constant&) {
      return true;
    },
    [&](const ast::unary_expr& x) {
      return x.expr.is_deterministic(reg);
    },
    [&](const ast::binary_expr& x) {
      return x.left.is_deterministic(reg) and x.right.is_deterministic(reg);
    },
    [&](const ast::function_call& x) {
      if (not reg.get(x).is_deterministic()) {
        return false;
      }
      return std::ranges::all_of(x.args, [&](auto& arg) {
        return arg.is_deterministic(reg);
      });
    },
    [](const auto&) {
      return false;
    });
}

} // namespace tenzir
