//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/active_rows.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/tql2/eval2.hpp"

#include <utility>

namespace tenzir2 {

class evaluator {
public:
  explicit evaluator(TableSlice input, tenzir::session ctx)
    : input_{std::move(input)},
      length_{input_.data_.length() == 0
                ? int64_t{1}
                : static_cast<int64_t>(input_.data_.length())},
      ctx_{ctx} {
  }

  auto eval(tenzir::ast::expression const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  /// Like `eval`, but skips evaluation and returns all-null if `active` has no
  /// active rows. Use this when `active` was narrowed down from a parent set,
  /// to avoid evaluating expressions in dead branches.
  auto eval_narrowed(tenzir::ast::expression const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
    if (active.as_constant() == false) {
      return null();
    }
    return eval(x, active);
  }

  auto eval(tenzir::ast::constant const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::record const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::list const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::this_ const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::root_field const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::function_call const& x,
            tenzir::ActiveRows const& active) -> array_<data>;

  auto eval(tenzir::ast::unary_expr const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::binary_expr const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::field_access const& x,
            tenzir::ActiveRows const& active) -> array_<data>;

  auto eval(tenzir::ast::assignment const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::meta const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::index_expr const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  auto eval(tenzir::ast::format_expr const& x, tenzir::ActiveRows const& active)
    -> array_<data>;

  template <class T>
    requires(
      tenzir::detail::tl_contains<tenzir::ast::expression_kinds, T>::value)
  auto eval(T const& x, tenzir::ActiveRows const& active) -> array_<data> {
    (void)active;
    return not_implemented(x);
  }

  auto eval(tenzir::ast::lambda_expr const& x, array_<list> const& input)
    -> array_<data>;

  auto to_array(data const& x) const -> array_<data>;

  auto input_or_throw(tenzir::into_location location) -> TableSlice const& {
    if (input_.data_.length() == 0) {
      tenzir::diagnostic::error("expected a constant expression")
        .primary(location)
        .emit(ctx_);
      throw tenzir::failure::promise();
    }
    return input_;
  }

  auto null() const -> array_<data> {
    return to_array(data{});
  }

  template <class T>
  auto not_implemented(T const& x) -> array_<data> {
    tenzir::diagnostic::warning("eval not implemented yet for: {:?}",
                                tenzir::use_default_formatter{x})
      .primary(x)
      .emit(ctx_);
    return null();
  }

  auto length() const -> int64_t {
    return length_;
  }

  auto ctx() const -> tenzir::session {
    return ctx_;
  }

private:
  TableSlice input_;
  int64_t length_;
  tenzir::session ctx_;
};

} // namespace tenzir2
