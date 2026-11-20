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

#include <charconv>
#include <cmath>
#include <limits>
#include <utility>
#include <vector>

namespace tenzir::pushdown {

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
  auto const* row = find_row(functions(), call.fn.ref.segments().front(),
                             std::span{call.args});
  if (not row) {
    return None{};
  }
  return renderer.spell(*row, call.args);
}

auto ExpressionDialect::time_literal_arithmetic() const -> bool {
  return true;
}

auto bucket_width(ast::expression const& expr) -> Option<duration> {
  auto const* constant = try_as<ast::constant>(expr);
  auto const* width = constant ? try_as<duration>(constant->value) : nullptr;
  using std::chrono::months, std::chrono::weeks;
  if (not width or *width <= duration::zero()
      or *width % weeks{1} == duration::zero()
      or *width % months{1} == duration::zero()) {
    return None{};
  }
  return *width;
}

auto ExpressionDialect::value_decode(ast::expression const& expr) const
  -> Option<Decode> {
  auto const* call = try_as<ast::function_call>(expr);
  if (not call or not call->fn.ref.resolved()
      or call->fn.ref.pkg() != entity_pkg_std
      or call->fn.ref.segments().size() != 1) {
    return Decode::native;
  }
  auto const* row = find_row(functions(), call->fn.ref.segments().front(),
                             std::span{call->args});
  return row ? row->decode : Decode::native;
}

auto aggregate_decode(
  ExpressionDialect const& dialect, ir::AggregateColumn const& column,
  folly::FunctionRef<auto(ast::expression const&)->Option<Decode>> decode_of)
  -> Option<Decode> {
  if (not column.function) {
    return decode_of(column.args.front());
  }
  if (*column.function == "min" or *column.function == "max") {
    if (auto kind = decode_of(column.args.front());
        kind and *kind != Decode::native) {
      return kind;
    }
  }
  auto const* row
    = find_row(dialect.aggregations(), *column.function, column.args.size());
  return row ? row->decode : Decode::native;
}

namespace {

/// The arguments of a call: the positional ones in order, and the named ones,
/// which are assignments to a plain name, by their name.
struct CallArguments {
  std::vector<Ref<ast::expression const>> positional;
  std::vector<std::pair<std::string, Ref<ast::expression const>>> named;
};

auto call_arguments(std::span<ast::expression const> args) -> CallArguments {
  auto result = CallArguments{};
  for (auto const& arg : args) {
    if (auto const* assignment = try_as<ast::assignment>(arg)) {
      auto const* field = try_as<ast::root_field>(assignment->left);
      result.named.emplace_back(field ? field->id.name : std::string{},
                                Ref{assignment->right});
      continue;
    }
    result.positional.emplace_back(arg);
  }
  return result;
}

/// A hole of a spelling: its argument, by index or by name, and its kind.
struct Hole {
  std::string_view argument;
  std::string_view kind;
};

/// Calls `f` with each hole of `spelling`, and with the text before it.
template <class F>
auto for_each_hole(std::string_view spelling, F f) -> void {
  while (not spelling.empty()) {
    auto const open = spelling.find('{');
    if (open == std::string_view::npos) {
      f(spelling, Option<Hole>{});
      return;
    }
    auto const close = spelling.find('}', open);
    TENZIR_ASSERT(close != std::string_view::npos);
    auto const inner = spelling.substr(open + 1, close - open - 1);
    auto const colon = inner.find(':');
    auto hole = Hole{
      .argument = inner.substr(0, colon),
      .kind = colon == std::string_view::npos ? std::string_view{}
                                              : inner.substr(colon + 1),
    };
    TENZIR_ASSERT(not hole.argument.empty());
    f(spelling.substr(0, open), Option{hole});
    spelling.remove_prefix(close + 1);
  }
}

auto is_index(std::string_view argument) -> bool {
  return std::ranges::all_of(argument, [](char c) {
    return c >= '0' and c <= '9';
  });
}

/// Whether a row with `spelling` and `arity` applies to a call with
/// `positional` positional arguments and the `named` ones, which must be
/// exactly the named arguments that its holes name.
auto spelling_fits(std::string_view spelling, size_t arity, size_t positional,
                   std::span<std::string const> named) -> bool {
  if (arity != positional) {
    return false;
  }
  auto holes = std::vector<std::string_view>{};
  for_each_hole(spelling, [&](std::string_view, Option<Hole> hole) {
    if (hole and not is_index(hole->argument)
        and not std::ranges::contains(holes, hole->argument)) {
      holes.push_back(hole->argument);
    }
  });
  return holes.size() == named.size()
         and std::ranges::all_of(named, [&](std::string const& name) {
               return std::ranges::contains(holes, std::string_view{name});
             });
}

} // namespace

auto find_row(std::span<Function const> rows, std::string_view name,
              std::span<ast::expression const> args) -> Function const* {
  auto const split = call_arguments(args);
  auto names = std::vector<std::string>{};
  for (auto const& [arg_name, _] : split.named) {
    names.push_back(arg_name);
  }
  auto it = std::ranges::find_if(rows, [&](Function const& row) {
    return row.tql == name
           and spelling_fits(row.spelling, row.arity, split.positional.size(),
                             names);
  });
  return it == rows.end() ? nullptr : &*it;
}

auto find_row(std::span<Function const> rows, std::string_view name,
              size_t arity) -> Function const* {
  auto it = std::ranges::find_if(rows, [&](Function const& row) {
    return row.tql == name
           and spelling_fits(row.spelling, row.arity, arity, {});
  });
  return it == rows.end() ? nullptr : &*it;
}

auto spell(
  std::string_view spelling, std::span<ast::expression const> args,
  folly::FunctionRef<auto(ast::expression const&, bool)->Option<std::string>>
    fill) -> Option<std::string> {
  auto const split = call_arguments(args);
  auto const argument = [&](std::string_view name) -> ast::expression const* {
    if (is_index(name)) {
      auto index = size_t{};
      std::from_chars(name.data(), name.data() + name.size(), index);
      return index < split.positional.size() ? &*split.positional[index]
                                             : nullptr;
    }
    for (auto const& [arg_name, value] : split.named) {
      if (arg_name == name) {
        return &*value;
      }
    }
    return nullptr;
  };
  auto result = std::string{};
  auto failed = false;
  for_each_hole(spelling, [&](std::string_view text, Option<Hole> hole) {
    if (failed) {
      return;
    }
    result += text;
    if (not hole) {
      return;
    }
    auto const* arg = argument(hole->argument);
    TENZIR_ASSERT(arg);
    auto const& kind = hole->kind;
    if (kind.empty() or kind == "pred") {
      auto text = fill(*arg, kind == "pred");
      if (not text) {
        failed = true;
        return;
      }
      result += *text;
      return;
    }
    if (kind == "nat") {
      auto const* constant = try_as<ast::constant>(*arg);
      auto const* value = constant ? try_as<int64_t>(constant->value) : nullptr;
      if (not value or *value < 0) {
        failed = true;
        return;
      }
      result += fmt::to_string(*value);
      return;
    }
    // A count of a unit, from the width of a time bucket that is a whole
    // multiple of it.
    auto const ticks = kind == "s"    ? duration{std::chrono::seconds{1}}
                       : kind == "us" ? duration{std::chrono::microseconds{1}}
                                      : duration{1};
    TENZIR_ASSERT(kind == "s" or kind == "us" or kind == "ns");
    auto width = bucket_width(*arg);
    if (not width or *width % ticks != duration::zero()) {
      failed = true;
      return;
    }
    result += fmt::to_string(*width / ticks);
  });
  if (failed) {
    return None{};
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
      if (not std::isfinite(x)) {
        return None{};
      }
      return wrap(dialect_->render_double(x));
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
  TRY(auto text, dialect_->render_call(x, *this));
  auto const* row = find_row(dialect_->functions(), ref.segments().front(),
                             std::span{x.args});
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
