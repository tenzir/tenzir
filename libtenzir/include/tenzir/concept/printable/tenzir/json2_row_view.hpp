//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"

#include <fmt/format.h>
#include <tenzir2/type_system/array/array.hpp>
#include <tenzir2/variant_traits.hpp>

#include <cmath>
#include <format>
#include <simdjson.h>
#include <string_view>

namespace tenzir {

/// Renders a single row of the experimental `tenzir2` data model
/// (`tenzir2::array_row_view_<data>`) to JSON.
///
/// This is the `tenzir2` counterpart to `json_printer2` (which prints the
/// classic `view3<data>`). It is intentionally minimal: it produces correct
/// JSON (quoting, escaping, `null`, nesting) but supports none of the
/// `json_printer_options` (null/empty omission, numeric durations, ...).
class json_printer2_row_view : printer_base<json_printer2_row_view> {
public:
  json_printer2_row_view() = default;

  auto load_new(tenzir2::array_row_view_<tenzir2::data> v) -> void {
    builder_.clear();
    print(v);
  }

  auto bytes() const noexcept -> std::span<const std::byte> {
    auto const v = builder_.view();
    return {
      reinterpret_cast<std::byte const*>(v->data()),
      v->size(),
    };
  }

private:
  auto print(tenzir2::array_row_view_<tenzir2::data> v) -> void {
    // Null-ness lives in `element_state`, not a dedicated variant branch. Treat
    // anything that is not `valid` (null, null_result, dead) as JSON `null`.
    if (not v.valid()) {
      builder_.append_null();
      return;
    }
    tenzir2::match(v, [&]<class T>(T row) {
      if constexpr (std::same_as<T, tenzir2::array_row_view_<tenzir2::null>>) {
        builder_.append_null();
      } else if constexpr (std::same_as<T, tenzir2::array_row_view_<bool>>) {
        builder_.append(*row);
      } else if constexpr (std::same_as<
                             T, tenzir2::array_row_view_<std::int64_t>>) {
        builder_.append(*row);
      } else if constexpr (std::same_as<
                             T, tenzir2::array_row_view_<std::uint64_t>>) {
        builder_.append(*row);
      } else if constexpr (std::same_as<T, tenzir2::array_row_view_<double>>) {
        print_double(*row);
      } else if constexpr (std::same_as<
                             T, tenzir2::array_row_view_<std::string>>) {
        // `*row` is a `std::string_view`.
        builder_.escape_and_append_with_quotes(*row);
      } else if constexpr (
        std::same_as<T, tenzir2::array_row_view_<tenzir2::ip>>
        or std::same_as<T, tenzir2::array_row_view_<tenzir2::subnet>>
        or std::same_as<T, tenzir2::array_row_view_<tenzir2::time>>
        or std::same_as<T, tenzir2::array_row_view_<tenzir2::duration>>) {
        // These `tenzir2` types provide `std::formatter` (not `fmt::formatter`)
        // specializations, so use `std::format` here.
        builder_.append('"');
        builder_.append_raw(std::format("{}", *row));
        builder_.append('"');
      } else if constexpr (std::same_as<
                             T, tenzir2::array_row_view_<tenzir2::list>>) {
        builder_.start_array();
        auto first = true;
        for (auto element : *row) {
          if (not first) {
            builder_.append_comma();
          }
          first = false;
          print(element);
        }
        builder_.end_array();
      } else if constexpr (std::same_as<
                             T, tenzir2::array_row_view_<tenzir2::record>>) {
        builder_.start_object();
        auto first = true;
        for (auto const& [key, value] : *row) {
          if (not first) {
            builder_.append_comma();
          }
          first = false;
          builder_.escape_and_append_with_quotes(key);
          builder_.append_colon();
          print(value);
        }
        builder_.end_object();
      } else {
        static_assert(false, "unhandled tenzir2 row view type");
      }
    });
  }

  auto print_double(double x) -> void {
    switch (std::fpclassify(x)) {
      case FP_NORMAL:
      case FP_SUBNORMAL:
      case FP_ZERO:
        break;
      default:
        builder_.append_null();
        return;
    }
    // There doesn't seem to be an option that prints `1.0` as `1.0`. It is
    // normally printed as `1`, or `1.` in alternate mode. Because we want to
    // have a trailing zero, we detect that case and append one. Detection
    // based purely on the fractional part doesn't work, because we might get
    // scientific notation and thereby produce incorrect JSON.
    auto result = fmt::to_string(x);
    auto is_integer = std::ranges::all_of(result, [](char ch) {
      return ('0' <= ch and ch <= '9') or ch == '-';
    });
    builder_.append_raw(result);
    if (is_integer) {
      builder_.append_raw(std::string_view{".0"});
    }
  }

  simdjson::builder::string_builder builder_;
};

} // namespace tenzir
