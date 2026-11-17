//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/expr_render.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>

#include <cmath>
#include <utility>
#include <vector>

namespace tenzir::pushdown {

auto ExpressionDialect::accepts_double(double value) const -> bool {
  TENZIR_UNUSED(value);
  return true;
}

auto ExpressionDialect::render_time(time value) const -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto ExpressionDialect::render_duration(duration value) const
  -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto ExpressionDialect::render_ip(ip value) const -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto ExpressionDialect::render_subnet(subnet value) const
  -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto ExpressionDialect::render_binary(ast::binary_expr const& x,
                                      ExpressionRenderer const& renderer) const
  -> Option<Option<std::string>> {
  TENZIR_UNUSED(x, renderer);
  return Option<std::string>{None{}};
}

auto ExpressionDialect::functions() const -> std::span<Function const> {
  return {};
}

auto ExpressionDialect::aggregations() const -> std::span<Aggregation const> {
  return {};
}

auto ExpressionDialect::render_call(ast::function_call const& call,
                                    ExpressionRenderer const& renderer) const
  -> Option<std::string> {
  auto const* row
    = find_row(functions(), call.fn.ref.segments().front(), call.args.size());
  if (not row) {
    return None{};
  }
  return renderer.spell(*row, call.args);
}

auto ExpressionDialect::time_literal_arithmetic() const -> bool {
  return true;
}

auto spell(std::string_view spelling, std::span<ast::expression const> args,
           folly::FunctionRef<auto(size_t, bool)->Option<std::string>> fill)
  -> Option<std::string> {
  auto result = std::string{};
  while (not spelling.empty()) {
    auto const open = spelling.find('{');
    result += spelling.substr(0, open);
    if (open == std::string_view::npos) {
      break;
    }
    auto const close = spelling.find('}', open);
    TENZIR_ASSERT(close != std::string_view::npos);
    auto hole = spelling.substr(open + 1, close - open - 1);
    auto const index = static_cast<size_t>(hole.front() - '0');
    TENZIR_ASSERT(index < args.size());
    auto const unit = hole.substr(hole.find(':') + 1);
    if (hole.size() < 2 or unit == "pred") {
      TRY(auto text, fill(index, hole.size() >= 2));
      result += text;
    } else {
      // A count of a unit, from a constant duration that is a whole, positive
      // multiple of it. TQL aligns multiples of weeks and months to the
      // calendar, so they have no count.
      auto const* constant = try_as<ast::constant>(args[index]);
      auto const* width
        = constant ? try_as<duration>(constant->value) : nullptr;
      auto const ticks = unit == "s"    ? duration{std::chrono::seconds{1}}
                         : unit == "ms" ? duration{std::chrono::milliseconds{1}}
                         : unit == "us" ? duration{std::chrono::microseconds{1}}
                                        : duration{1};
      using std::chrono::months, std::chrono::weeks;
      if (not width or *width <= duration::zero()
          or *width % ticks != duration{} or *width % weeks{1} == duration{}
          or *width % months{1} == duration{}) {
        return None{};
      }
      result += fmt::to_string(*width / ticks);
    }
    spelling.remove_prefix(close + 1);
  }
  return result;
}

ExpressionRenderer::ExpressionRenderer(ExpressionDialect const& dialect,
                                       Binder bind)
  : dialect_{dialect}, bind_{std::move(bind)} {
}

auto ExpressionRenderer::predicate(ast::expression const& expr) const
  -> Option<std::string> {
  TRY(auto result, render(expr));
  if (not result.predicate and not dialect_->boolean_values()) {
    return None{};
  }
  return std::move(result.text);
}

auto ExpressionRenderer::value(ast::expression const& expr) const
  -> Option<std::string> {
  TRY(auto result, render(expr));
  if (result.predicate and not dialect_->boolean_values()) {
    return None{};
  }
  return std::move(result.text);
}

auto ExpressionRenderer::literal(ast::constant::kind const& value) const
  -> Option<std::string> {
  TRY(auto result, render_literal(value));
  return std::move(result.text);
}

auto ExpressionRenderer::render_literal(ast::constant::kind const& value) const
  -> Option<Fragment> {
  auto wrap = [](Option<std::string> text) -> Option<Fragment> {
    TRY(auto x, std::move(text));
    return Fragment{std::move(x)};
  };
  return match(
    value,
    [&](caf::none_t) -> Option<Fragment> {
      return wrap(dialect_->render_null());
    },
    [&](bool x) -> Option<Fragment> {
      return Fragment{dialect_->render_bool(x)};
    },
    [&](int64_t x) -> Option<Fragment> {
      return wrap(dialect_->render_int(x));
    },
    [&](uint64_t x) -> Option<Fragment> {
      return wrap(dialect_->render_uint(x));
    },
    [&](double x) -> Option<Fragment> {
      if (not std::isfinite(x) or not dialect_->accepts_double(x)) {
        return None{};
      }
      return Fragment{dialect_->render_double(x)};
    },
    [&](std::string const& x) -> Option<Fragment> {
      return wrap(dialect_->quote_string(x));
    },
    [&](time x) -> Option<Fragment> {
      return wrap(dialect_->render_time(x));
    },
    [&](duration x) -> Option<Fragment> {
      return wrap(dialect_->render_duration(x));
    },
    [&](ip const& x) -> Option<Fragment> {
      return wrap(dialect_->render_ip(x));
    },
    [&](subnet const& x) -> Option<Fragment> {
      return wrap(dialect_->render_subnet(x));
    },
    [](auto const&) -> Option<Fragment> {
      return None{};
    });
}

auto ExpressionRenderer::field(ast::expression const& expr) const
  -> Option<Fragment> {
  TRY(auto path, ast::field_path::try_from(expr));
  auto names = std::vector<std::string>{};
  for (auto const& segment : path.path()) {
    if (segment.has_question_mark) {
      return None{};
    }
    names.push_back(segment.id.name);
  }
  if (names.empty()) {
    return None{};
  }
  TRY(auto text, bind_(names));
  return Fragment{std::move(text)};
}

auto ExpressionRenderer::list(ast::expression const& expr) const
  -> Option<std::vector<std::string>> {
  auto elements = std::vector<std::string>{};
  if (auto const* xs = try_as<ast::list>(expr)) {
    for (auto const& item : xs->items) {
      auto const* element = try_as<ast::expression>(item);
      if (not element) {
        return None{};
      }
      auto const* constant = try_as<ast::constant>(*element);
      if (not constant) {
        return None{};
      }
      TRY(auto x, render_literal(constant->value));
      elements.push_back(std::move(x.text));
    }
  } else if (auto const* constant = try_as<ast::constant>(expr)) {
    // The optimizer folds a list of constants into one constant.
    auto const* xs = try_as<tenzir::list>(constant->value);
    if (not xs) {
      return None{};
    }
    for (auto const& element : *xs) {
      auto folded = ast::constant::make(located{element, location::unknown});
      TRY(auto x, render_literal(folded.value));
      elements.push_back(std::move(x.text));
    }
  } else {
    return None{};
  }
  if (elements.empty()) {
    return None{};
  }
  return elements;
}

namespace {

auto is_null(ast::expression const& expr) -> bool {
  auto const* constant = try_as<ast::constant>(expr);
  return constant and is<caf::none_t>(constant->value);
}

auto is_time(ast::expression const& expr) -> bool {
  auto const* constant = try_as<ast::constant>(expr);
  return constant and is<time>(constant->value);
}

} // namespace

auto ExpressionRenderer::binary(ast::binary_expr const& x) const
  -> Option<Fragment> {
  using enum ast::binary_op;
  TRY(auto custom, dialect_->render_binary(x, *this));
  if (custom) {
    auto const arithmetic
      = x.op == add or x.op == sub or x.op == mul or x.op == div;
    return Fragment{std::move(*custom), not arithmetic};
  }
  auto const spell = [&](bool predicate, bool logical) -> Option<Fragment> {
    TRY(auto op, dialect_->binary_operator(x.op));
    auto operand = [&](ast::expression const& e) {
      return logical ? this->predicate(e) : value(e);
    };
    TRY(auto left, operand(x.left));
    TRY(auto right, operand(x.right));
    return Fragment{fmt::format("({} {} {})", left, op, right), predicate};
  };
  switch (x.op) {
    case eq:
    case neq: {
      auto const negated = x.op == neq;
      for (auto const* side : {&x.left, &x.right}) {
        if (is_null(*side)) {
          TRY(auto operand, value(side == &x.left ? x.right : x.left));
          TRY(auto text, dialect_->render_null_check(operand, negated));
          return Fragment{std::move(text), true};
        }
      }
      return spell(true, false);
    }
    case lt:
    case leq:
    case gt:
    case geq:
      return spell(true, false);
    case add:
    case sub:
    case mul:
    case div:
      if (not dialect_->time_literal_arithmetic()
          and (is_time(x.left) or is_time(x.right))) {
        return None{};
      }
      return spell(false, false);
    case and_:
    case or_:
      return spell(true, true);
    case in: {
      TRY(auto left, value(x.left));
      TRY(auto right, list(x.right));
      TRY(auto text, dialect_->render_in(left, right));
      return Fragment{std::move(text), true};
    }
    case if_:
    case else_:
      return None{};
  }
  TENZIR_UNREACHABLE();
}

auto ExpressionRenderer::call(ast::function_call const& x) const
  -> Option<Fragment> {
  auto const& ref = x.fn.ref;
  if (not ref.resolved() or ref.pkg() != entity_pkg_std
      or ref.segments().size() != 1) {
    return None{};
  }
  // Named arguments are assignments, which have no spelling as a value.
  TRY(auto text, dialect_->render_call(x, *this));
  auto const* row
    = find_row(dialect_->functions(), ref.segments().front(), x.args.size());
  return Fragment{std::move(text), row and row->predicate};
}

auto ExpressionRenderer::render(ast::expression const& expr) const
  -> Option<Fragment> {
  if (auto const* constant = try_as<ast::constant>(expr)) {
    return render_literal(constant->value);
  }
  if (auto const* x = try_as<ast::binary_expr>(expr)) {
    return binary(*x);
  }
  if (auto const* x = try_as<ast::unary_expr>(expr)) {
    if (is_time(x->expr) and not dialect_->time_literal_arithmetic()) {
      return None{};
    }
    if (x->op == ast::unary_op::not_) {
      TRY(auto operand, predicate(x->expr));
      TRY(auto text, dialect_->render_not(operand));
      return Fragment{std::move(text), true};
    }
    if (x->op == ast::unary_op::neg) {
      TRY(auto operand, value(x->expr));
      TRY(auto text, dialect_->render_negate(operand));
      return Fragment{std::move(text)};
    }
    return None{};
  }
  if (auto const* x = try_as<ast::function_call>(expr)) {
    return call(*x);
  }
  return field(expr);
}

} // namespace tenzir::pushdown
