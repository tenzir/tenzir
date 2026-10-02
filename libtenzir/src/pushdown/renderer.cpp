//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/renderer.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <functional>
#include <vector>

namespace tenzir::pushdown {

namespace {

auto spell(BinaryOp op) -> std::string_view {
  switch (op) {
    case BinaryOp::eq:
      return "=";
    case BinaryOp::neq:
      return "!=";
    case BinaryOp::lt:
      return "<";
    case BinaryOp::leq:
      return "<=";
    case BinaryOp::gt:
      return ">";
    case BinaryOp::geq:
      return ">=";
    case BinaryOp::add:
      return "+";
    case BinaryOp::sub:
      return "-";
    case BinaryOp::mul:
      return "*";
    case BinaryOp::div:
      return "/";
  }
  TENZIR_UNREACHABLE();
}

auto is_arithmetic(BinaryOp op) -> bool {
  switch (op) {
    case BinaryOp::add:
    case BinaryOp::sub:
    case BinaryOp::mul:
    case BinaryOp::div:
      return true;
    default:
      return false;
  }
}

} // namespace

auto SqlRenderer::render(Expr const& expr) const -> Option<std::string> {
  TRY(auto result, render_fragment(expr));
  return wrap(result, Slot::logical);
}

auto SqlRenderer::render_enum(EnumLabel const&) const -> Option<Fragment> {
  return None{};
}

auto SqlRenderer::render_time(TimeValue const&) const -> Option<Fragment> {
  return None{};
}

auto SqlRenderer::render_ip(IpValue const&) const -> Option<Fragment> {
  return None{};
}

auto SqlRenderer::render_call(Operation, std::span<Fragment const>) const
  -> Option<Fragment> {
  return None{};
}

auto SqlRenderer::render_comparison(Binary const& x, Fragment const& left,
                                    Fragment const& right) const
  -> Option<Fragment> {
  return sql_binary(spell(x.op), left, right, Fragment::Kind::predicate);
}

auto SqlRenderer::render_arithmetic(Binary const& x, Fragment const& left,
                                    Fragment const& right) const
  -> Option<Fragment> {
  return sql_binary(spell(x.op), left, right, Fragment::Kind::arithmetic);
}

auto SqlRenderer::render_conditional(Fragment const& condition,
                                     Fragment const& then,
                                     Fragment const& otherwise) const
  -> Option<Fragment> {
  return sql_conditional(condition, then, otherwise);
}

auto SqlRenderer::sql_atom(std::string text) const -> Fragment {
  return {std::move(text), Fragment::Kind::atom};
}

auto SqlRenderer::sql_call(std::string_view name,
                           std::span<Fragment const> args) const -> Fragment {
  auto parts = std::vector<std::string>{};
  for (auto const& arg : args) {
    parts.push_back(wrap(arg, Slot::delimited));
  }
  return sql_atom(fmt::format("{}({})", name, fmt::join(parts, ", ")));
}

auto SqlRenderer::sql_binary(std::string_view op, Fragment const& left,
                             Fragment const& right, Fragment::Kind kind) const
  -> Fragment {
  TENZIR_ASSERT(kind == Fragment::Kind::predicate
                or kind == Fragment::Kind::arithmetic);
  return {fmt::format("{} {} {}", wrap(left, Slot::operand), op,
                      wrap(right, Slot::operand)),
          kind};
}

auto SqlRenderer::sql_junction(std::string_view keyword,
                               std::span<Fragment const> operands) const
  -> Fragment {
  TENZIR_ASSERT(operands.size() >= 2);
  auto parts = std::vector<std::string>{};
  for (auto const& operand : operands) {
    parts.push_back(wrap(operand, Slot::logical));
  }
  return {fmt::format("{}", fmt::join(parts, fmt::format(" {} ", keyword))),
          Fragment::Kind::junction};
}

auto SqlRenderer::sql_not(Fragment const& operand) const -> Fragment {
  return {fmt::format("NOT {}", wrap(operand, Slot::logical)),
          Fragment::Kind::predicate};
}

auto SqlRenderer::sql_conditional(Fragment const& condition,
                                  Fragment const& then,
                                  Fragment const& otherwise) const -> Fragment {
  return sql_atom(fmt::format(
    "CASE WHEN {} THEN {} ELSE {} END", wrap(condition, Slot::delimited),
    wrap(then, Slot::delimited), wrap(otherwise, Slot::delimited)));
}

auto SqlRenderer::wrap(Fragment const& child, Slot slot) const -> std::string {
  auto grouped = std::invoke([&] {
    switch (child.kind) {
      case Fragment::Kind::atom:
        return false;
      case Fragment::Kind::predicate:
        return slot == Slot::operand;
      case Fragment::Kind::junction:
      case Fragment::Kind::arithmetic:
        return slot != Slot::delimited;
    }
    TENZIR_UNREACHABLE();
  });
  if (grouped) {
    return fmt::format("({})", child.text);
  }
  return child.text;
}

auto SqlRenderer::render_fragment(Expr const& expr) const -> Option<Fragment> {
  // Renders each of `exprs`, or vetoes if one of them is vetoed.
  auto render_all
    = [&](std::span<Expr const> exprs) -> Option<std::vector<Fragment>> {
    auto result = std::vector<Fragment>{};
    result.reserve(exprs.size());
    for (auto const& x : exprs) {
      TRY(auto fragment, render_fragment(x));
      result.push_back(std::move(fragment));
    }
    return result;
  };
  auto predicate = [](std::string text) -> Fragment {
    return {std::move(text), Fragment::Kind::predicate};
  };
  return match(
    expr,
    [&](Literal const& x) -> Option<Fragment> {
      return render_literal(x);
    },
    [&](Column const& x) -> Option<Fragment> {
      TENZIR_ASSERT(not x.path.empty());
      auto result = std::string{};
      for (auto const& segment : x.path) {
        if (not result.empty()) {
          result += '.';
        }
        result += quote_identifier(segment);
      }
      return sql_atom(std::move(result));
    },
    [&](Call const& x) -> Option<Fragment> {
      TRY(auto args, render_all(x.args));
      return render_call(x.op, args);
    },
    [&](Binary const& x) -> Option<Fragment> {
      TRY(auto left, render_fragment(*x.left));
      TRY(auto right, render_fragment(*x.right));
      if (is_arithmetic(x.op)) {
        return render_arithmetic(x, left, right);
      }
      return render_comparison(x, left, right);
    },
    [&](And const& x) -> Option<Fragment> {
      TRY(auto operands, render_all(x.operands));
      return sql_junction("AND", operands);
    },
    [&](Or const& x) -> Option<Fragment> {
      TRY(auto operands, render_all(x.operands));
      return sql_junction("OR", operands);
    },
    [&](Not const& x) -> Option<Fragment> {
      TRY(auto inner, render_fragment(*x.expr));
      return sql_not(inner);
    },
    [&](In const& x) -> Option<Fragment> {
      // SQL has no empty list.
      if (x.list.empty()) {
        return None{};
      }
      TRY(auto inner, render_fragment(*x.expr));
      TRY(auto elements, render_all(x.list));
      auto list = std::vector<std::string>{};
      for (auto const& element : elements) {
        list.push_back(wrap(element, Slot::delimited));
      }
      return predicate(fmt::format("{} IN ({})", wrap(inner, Slot::operand),
                                   fmt::join(list, ", ")));
    },
    [&](Between const& x) -> Option<Fragment> {
      TRY(auto inner, render_fragment(*x.expr));
      TRY(auto lo, render_fragment(*x.lo));
      TRY(auto hi, render_fragment(*x.hi));
      return predicate(
        fmt::format("{} BETWEEN {} AND {}", wrap(inner, Slot::operand),
                    wrap(lo, Slot::operand), wrap(hi, Slot::operand)));
    },
    [&](IsNull const& x) -> Option<Fragment> {
      TRY(auto inner, render_fragment(*x.expr));
      return predicate(fmt::format("{} IS {}NULL", wrap(inner, Slot::operand),
                                   x.negated ? "NOT " : ""));
    },
    [&](Conditional const& x) -> Option<Fragment> {
      TRY(auto condition, render_fragment(*x.condition));
      TRY(auto then, render_fragment(*x.then));
      TRY(auto otherwise, render_fragment(*x.otherwise));
      return render_conditional(condition, then, otherwise);
    });
}

auto SqlRenderer::render_literal(Literal const& x) const -> Option<Fragment> {
  return match(
    x.value,
    [&](Null const&) -> Option<Fragment> {
      return sql_atom("NULL");
    },
    [&](bool y) -> Option<Fragment> {
      return sql_atom(y ? "true" : "false");
    },
    [&](int64_t y) -> Option<Fragment> {
      return sql_atom(fmt::to_string(y));
    },
    [&](uint64_t y) -> Option<Fragment> {
      return sql_atom(fmt::to_string(y));
    },
    [&](double y) -> Option<Fragment> {
      return sql_atom(render_double(y));
    },
    [&](std::string const& y) -> Option<Fragment> {
      return sql_atom(quote_string(y));
    },
    [&](EnumLabel const& y) -> Option<Fragment> {
      return render_enum(y);
    },
    [&](TimeValue const& y) -> Option<Fragment> {
      return render_time(y);
    },
    [&](IpValue const& y) -> Option<Fragment> {
      return render_ip(y);
    });
}

} // namespace tenzir::pushdown
