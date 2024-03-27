//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::tql2 {

namespace {

class evaluator {
public:
  explicit evaluator(context& ctx) : ctx_{ctx} {
  }

  auto eval(const ast::expression& x) -> data {
    return x.match([&](auto& y) {
      return eval(y);
    });
  }

  auto eval(const ast::record& x) -> data {
    auto result = record{};
    for (auto& y : x.content) {
      y.match(
        [&](const ast::record::field& y) {
          auto val = eval(y.expr);
          auto inserted = result.emplace(y.name.name, val).second;
          if (not inserted) {
            diagnostic::error("field `{}` already exists", y.name.name)
              .primary(y.name.location)
              .throw_();
          }
        },
        [](const auto&) {
          // TODO
          diagnostic::error("not implemented").throw_();
        });
    }
    return result;
  }

  auto eval(const ast::selector& x) -> data {
    diagnostic::error("expected a constant expression")
      .primary(x.get_location())
      .throw_();
  }

  auto eval(const ast::literal& x) -> data {
    return x.value.match(
      [&](const auto& y) -> data {
        return y;
      },
      [&](ast::null) -> data {
        return caf::none;
      });
  }

  auto eval(const ast::unary_expr& x) -> data {
    // TODO
    auto val = eval(x.expr);
    auto not_implemented = [&] {
      diagnostic::error("unary op eval not implemented")
        .primary(x.get_location())
        .throw_();
    };
    return caf::visit(
      [&]<class T>(const T& y) -> data {
        if constexpr (std::signed_integral<T> || std::floating_point<T>) {
          if (x.op.inner != ast::unary_op::neg) {
            not_implemented();
          }
          return -y;
        }
        not_implemented();
        TENZIR_UNREACHABLE();
      },
      val);
  }

  auto eval(const ast::function_call& x) -> data {
    if (not x.fn.ref.resolved()) {
      throw std::monostate{};
    }
    auto& entity = ctx_.reg().get(x.fn.ref);
    auto fn = std::get_if<std::unique_ptr<function_def>>(&entity);
    // TODO
    TENZIR_ASSERT(fn);
    TENZIR_ASSERT(*fn);
    auto args = std::vector<located<data>>{};
    args.reserve(x.args.size());
    for (auto& arg : x.args) {
      auto val = eval(arg);
      args.emplace_back(val, arg.get_location());
    }
    auto result = (*fn)->evaluate(x.fn.get_location(), std::move(args), ctx_);
    if (not result) {
      throw std::monostate{};
    }
    return std::move(*result);
  }

  template <class T>
  auto eval(const T& x) -> data {
    diagnostic::error("not implemented").primary(x.get_location()).throw_();
  }

private:
  [[maybe_unused]] context& ctx_;
};

} // namespace

auto evaluate(const ast::expression& expr, context& ctx)
  -> std::optional<data> {
  try {
    return evaluator{ctx}.eval(expr);
  } catch (diagnostic& d) {
    ctx.dh().emit(std::move(d));
    // TODO
    return std::nullopt;
  } catch (std::monostate) {
    return std::nullopt;
  }
}

} // namespace tenzir::tql2
