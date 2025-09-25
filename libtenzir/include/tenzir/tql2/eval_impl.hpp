//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

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
      TENZIR_ASSERT(begin == 0);
      TENZIR_ASSERT(end == 1);
      return result;
    }
    TENZIR_ASSERT(begin >= 0, "{} (begin) >= 0", begin);
    TENZIR_ASSERT(end >= begin, "{} (end) >= {} (begin)", end, begin);
    TENZIR_ASSERT(static_cast<size_t>(end) <= input->rows(),
                  "{} (end) <= {} (rows)", end, input->rows());
    result.input_ = subslice(*input, begin, end);
    result.length_ = end - begin;
    return result;
  }

  auto eval(const ast::expression& x) -> multi_series;

  auto eval(const ast::constant& x) -> multi_series;

  auto eval(const ast::record& x) -> multi_series;

  auto eval(const ast::list& x) -> multi_series;

  auto eval(const ast::this_& x) -> multi_series;

  auto eval(const ast::root_field& x) -> multi_series;

  auto eval(const ast::function_call& x) -> multi_series;

  auto eval(const ast::unary_expr& x) -> multi_series;

  auto eval(const ast::binary_expr& x) -> multi_series;

  auto eval(const ast::field_access& x) -> multi_series;

  auto eval(const ast::assignment& x) -> multi_series;

  auto eval(const ast::meta& x) -> multi_series;

  auto eval(const ast::index_expr& x) -> multi_series;

  auto eval(const ast::format_expr& x) -> multi_series;

  template <class T>
    requires(detail::tl_contains<ast::expression_kinds, T>::value)
  auto eval(const T& x) -> multi_series {
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
