//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/session.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"

#include <arrow/api.h>

namespace tenzir {

inline auto data_to_series(const data& x, int64_t length) -> series {
  // TODO: This is overkill.
  auto b = series_builder{};
  for (auto i = int64_t{0}; i < length; ++i) {
    b.data(x);
  }
  return b.finish_assert_one_array();
}

class evaluator {
public:
  explicit evaluator(const table_slice* input, session ctx)
    : input_{input},
      length_{input ? detail::narrow<int64_t>(input->rows()) : 1},
      ctx_{ctx} {
  }

  auto to_series(const data& x) -> series {
    return data_to_series(x, length_);
  }

  // TODO: This is pretty bad.
  auto input_or_throw(as_location location) -> const table_slice& {
    if (not input_) {
      diagnostic::error("expected a constant expression")
        .primary(location)
        .emit(ctx_);
      throw std::monostate{};
    }
    return *input_;
  }

  auto null() -> series {
    return to_series(caf::none);
  }

  auto eval(const ast::expression& x) -> series {
    return x.match([&](auto& y) {
      return eval(y);
    });
  }

  auto eval(const ast::constant& x) -> series;

  auto eval(const ast::record& x) -> series;

  auto eval(const ast::list& x) -> series;

  auto eval(const ast::this_& x) -> series;

  auto eval(const ast::root_field& x) -> series;

  auto eval(const ast::function_call& x) -> series;

  auto eval(const ast::unary_expr& x) -> series;

  auto eval(const ast::binary_expr& x) -> series;

  auto eval(const ast::field_access& x) -> series;

  auto eval(const ast::assignment& x) -> series;

  auto eval(const ast::meta& x) -> series;

  template <class T>
  auto eval(const T& x) -> series {
    return not_implemented(x);
  }

  auto not_implemented(const auto& x) -> series {
    diagnostic::warning("eval not implemented yet for: {:?}",
                        use_default_formatter(x))
      .primary(x)
      .emit(ctx_);
    return null();
  }

  auto length() const -> int64_t {
    return length_;
  }

private:
  const table_slice* input_;
  int64_t length_;
  session ctx_;
};

} // namespace tenzir
