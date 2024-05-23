//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/enumerate.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <arrow/api.h>

namespace tenzir::tql2 {

inline void ensure(const arrow::Status& status) {
  TENZIR_ASSERT(status.ok(), status.ToString());
}

template <class T>
[[nodiscard]] auto ensure(arrow::Result<T> result) -> T {
  ensure(result.status());
  return result.MoveValueUnsafe();
}

template <std::derived_from<arrow::ArrayBuilder> T>
[[nodiscard]] auto finish(T& x) {
  using Type = std::conditional_t<std::same_as<arrow::StringBuilder, T>,
                                  arrow::StringType, typename T::TypeClass>;
  auto result = std::shared_ptr<typename arrow::TypeTraits<Type>::ArrayType>{};
  ensure(x.Finish(&result));
  TENZIR_ASSERT(result);
  return result;
}

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
  explicit evaluator(const table_slice* input, diagnostic_handler& dh)
    : input_{input},
      length_{input ? detail::narrow<int64_t>(input->rows()) : 1},
      dh_{dh} {
  }

  auto to_series(const data& x) -> series {
    return data_to_series(x, length_);
  }

  // TODO: This is pretty bad.
  auto input_or_throw(location location) -> const table_slice& {
    if (not input_) {
      diagnostic::error("expected a constant expression")
        .primary(location)
        .emit(dh_);
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

  auto eval(const ast::literal& x) -> series {
    return to_series(x.as_data());
  }

  auto eval(const ast::record& x) -> series;

  auto eval(const ast::list& x) -> series;

  // auto eval(const ast::path& x) -> series;

  auto eval(const ast::function_call& x) -> series;

  auto eval(const ast::unary_expr& x) -> series;

  auto eval(const ast::binary_expr& x) -> series;

  auto eval(const ast::field_access& x) -> series;

  auto eval(const ast::assignment& x) -> series {
    diagnostic::warning("unexpected assignment")
      .primary(x.get_location())
      .emit(dh_);
    return null();
  }

  auto eval(const ast::meta& x) -> series {
    // TODO: This is quite inefficient.
    auto& input = input_or_throw(x.get_location());
    switch (x.kind) {
      case meta_extractor::schema:
        return to_series(std::string{input.schema().name()});
      case meta_extractor::schema_id:
        return to_series(input.schema().make_fingerprint());
      case meta_extractor::import_time:
        return to_series(input.import_time());
      case meta_extractor::internal:
        return to_series(input.schema().attribute("internal").has_value());
    }
    TENZIR_UNREACHABLE();
  }

  auto eval(const auto& x) -> series {
    return not_implemented(x);
  }

  auto not_implemented(const auto& x) -> series {
    diagnostic::warning("eval not implemented yet for: {:?}",
                        use_default_formatter(x))
      .primary(x.get_location())
      .emit(dh_);
    return null();
  }

private:
  const table_slice* input_;
  int64_t length_;
  diagnostic_handler& dh_;
};
} // namespace tenzir::tql2
