//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"

#include <concepts>
#include <functional>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

namespace tenzir::detail {

template <typename T>
auto eval_as(std::string_view name, ast::expression const& expr,
             table_slice const& slice, diagnostic_handler& dh)
  -> generator<std::optional<view3<type_to_data_t<T>>>> {
  return eval_as<T>(name, expr, slice, dh, [] {
    return std::nullopt;
  });
}

template <typename T, class MakeDefault>
auto eval_as(std::string_view name, ast::expression const& expr,
             table_slice const& slice, diagnostic_handler& dh,
             MakeDefault&& make_default)
  -> generator<std::optional<view3<type_to_data_t<T>>>> {
  auto ms = std::invoke([&] {
    if (expr.get_location()) {
      return eval(expr, slice, dh);
    }
    auto ndh = null_diagnostic_handler{};
    return eval(expr, slice, ndh);
  });
  for (auto const& s : ms.parts()) {
    if (s.type.kind().template is<null_type>()) {
      for (auto i = int64_t{}; i < s.length(); ++i) {
        co_yield make_default();
      }
      continue;
    }
    if (s.type.kind().template is<T>()) {
      for (auto val : s.template values<T>()) {
        if (val) {
          co_yield std::move(*val);
        } else {
          co_yield make_default();
        }
      }
      continue;
    }
    if constexpr (concepts::one_of<T, int64_type, uint64_type>) {
      using alt_type = std::conditional_t<std::same_as<T, int64_type>,
                                          uint64_type, int64_type>;
      if (s.type.kind().template is<alt_type>()) {
        auto overflow_warned = false;
        for (auto val : s.template values<alt_type>()) {
          if (not val) {
            co_yield make_default();
            continue;
          }
          if (not std::in_range<decltype(T::construct())>(*val)) {
            if (not overflow_warned) {
              overflow_warned = true;
              diagnostic::warning("overflow in `{}`, got `{}`", name, *val)
                .primary(expr)
                .emit(dh);
            }
            co_yield make_default();
            continue;
          }
          co_yield *val;
        }
        continue;
      }
    }
    diagnostic::warning("`{}` must be `{}`, got `{}`", name, T{}, s.type.kind())
      .primary(expr)
      .emit(dh);
    for (auto i = int64_t{}; i < s.length(); ++i) {
      co_yield make_default();
    }
  }
}

} // namespace tenzir::detail
