//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/active_rows.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/series.hpp"
#include "tenzir/session.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"

#include <arrow/api.h>

namespace tenzir {

class evaluator {
public:
  explicit evaluator(const table_slice* input, session ctx)
    : input_{input},
      length_{input ? detail::narrow<int64_t>(input->rows()) : 1},
      ctx_{ctx} {
  }

  auto slice(int64_t begin, int64_t end) const -> evaluator {
    auto result = *this;
    if (begin == 0 and end == length_) {
      return result;
    }
    const auto* input = result.get_input();
    if (not input) {
      TENZIR_ASSERT_EQ(begin, 0);
      TENZIR_ASSERT_EQ(end, 1);
      return result;
    }
    TENZIR_ASSERT_GEQ(begin, 0);
    TENZIR_ASSERT_GEQ(end, begin);
    TENZIR_ASSERT_LEQ(static_cast<size_t>(end), input->rows());
    result.input_ = subslice(*input, begin, end);
    result.length_ = end - begin;
    return result;
  }

  auto eval(ast::expression const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::constant const& x, ActiveRows const& active) -> multi_series;

  auto eval(ast::record const& x, ActiveRows const& active) -> multi_series;

  auto eval(ast::list const& x, ActiveRows const& active) -> multi_series;

  auto eval(ast::this_ const& x, ActiveRows const& active) -> multi_series;

  auto eval(ast::root_field const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::function_call const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::unary_expr const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::binary_expr const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::field_access const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::assignment const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::meta const& x, ActiveRows const& active) -> multi_series;

  auto eval(ast::index_expr const& x, ActiveRows const& active)
    -> multi_series;

  auto eval(ast::format_expr const& x, ActiveRows const& active)
    -> multi_series;

  template <class T>
    requires(detail::tl_contains<ast::expression_kinds, T>::value)
  auto eval(T const& x, ActiveRows const& active) -> multi_series {
    TENZIR_UNUSED(active);
    return not_implemented(x);
  }

  auto eval(const ast::lambda_expr& x, const basic_series<list_type>& input)
    -> multi_series;

  auto to_series(const data& x) const -> series;

  auto input_or_throw(into_location location) -> const table_slice&;

  auto null() const -> series {
    return to_series(caf::none);
  }

  template <class T>
  auto not_implemented(const T& x) -> series {
    diagnostic::warning("eval not implemented yet for: {:?}",
                        use_default_formatter(x))
      .primary(x)
      .emit(ctx_);
    return null();
  }

  auto length() const -> int64_t {
    return length_;
  }

  auto get_input() const -> const table_slice* {
    return input_.match(
      [](const table_slice* input) {
        return input;
      },
      [](const table_slice& input) {
        return &input;
      });
  }

  auto ctx() const -> session {
    return ctx_;
  }

private:
  variant<const table_slice*, table_slice> input_;
  int64_t length_;
  session ctx_;
};

} // namespace tenzir
