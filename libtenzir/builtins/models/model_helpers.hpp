//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/diagnostics.hpp>
#include <tenzir/series.hpp>
#include <tenzir/session.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/view3.hpp>

#include <cstdint>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::detail {

/// Extracts per-row numeric query values from a series. Returns an empty
/// optional for an unsupported type.
inline auto extract_numbers(series const& xs)
  -> Option<std::vector<Option<double>>> {
  auto result = std::vector<Option<double>>{};
  result.reserve(xs.length());
  auto const supported = match(
    xs.type,
    [&]<concepts::one_of<double_type, int64_type, uint64_type> Type>(
      Type const&) {
      auto const& array = as<type_to_arrow_array_t<Type>>(*xs.array);
      for (auto value : values3(array)) {
        result.emplace_back(value ? Option{static_cast<double>(*value)}
                                  : None{});
      }
      return true;
    },
    [&](null_type const&) {
      result.resize(xs.length());
      return true;
    },
    [&](auto const&) {
      return false;
    });
  return supported ? Option{std::move(result)} : None{};
}

/// Parses every non-null model record and invokes `f(model, row)`, where
/// `model` is empty when the row has no usable model. Emits at most one
/// diagnostic per batch for non-record input or malformed models.
template <class ParseModel, class F>
auto for_each_model(series const& models, ast::expression const& expr,
                    session ctx, bool& warned, std::string_view model_name,
                    ParseModel&& parse_model, F&& f) -> void {
  using parse_result = std::invoke_result_t<ParseModel&, record_view3>;
  using model_type
    = std::remove_cvref_t<decltype(std::declval<parse_result>().unwrap())>;
  if (is<null_type>(models.type)) {
    for (auto row = int64_t{0}; row < models.length(); ++row) {
      f(None{}, row);
    }
    return;
  }
  auto const records = models.as<record_type>();
  if (not records) {
    if (not warned) {
      warned = true;
      diagnostic::warning("expected `record`, got `{}`", models.type.kind())
        .primary(expr.get_location().subloc(0, 1))
        .emit(ctx);
    }
    for (auto row = int64_t{0}; row < models.length(); ++row) {
      f(None{}, row);
    }
    return;
  }
  auto row = int64_t{0};
  for (auto value : values3(*records->array)) {
    if (not value) {
      f(None{}, row++);
      continue;
    }
    auto parsed = parse_model(*value);
    if (not parsed) {
      if (not warned) {
        warned = true;
        diagnostic::warning("malformed {} model: {}", model_name,
                            parsed.unwrap_err())
          .primary(expr.get_location().subloc(0, 1))
          .emit(ctx);
      }
      f(None{}, row++);
      continue;
    }
    auto parsed_model = std::move(parsed).unwrap();
    f(Option<model_type const&>{parsed_model}, row++);
  }
}

} // namespace tenzir::detail
