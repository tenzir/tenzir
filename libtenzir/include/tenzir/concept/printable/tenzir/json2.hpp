//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/as_bytes.hpp"
#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/tenzir/json_printer_options.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/view3.hpp"

#include <fmt/format.h>

#include <simdjson.h>
#include <string_view>

namespace tenzir {

class json_printer2 : printer_base<json_printer2> {
public:
  explicit json_printer2(json_printer_options options)
    : printer_base(), options_{options} {
    // nop
  }

  auto load_new(view3<data> v) -> void {
    builder_.clear();
    match(v, *this);
  }

  auto bytes() const noexcept -> std::span<const std::byte> {
    auto const v = builder_.view();
    return {
      reinterpret_cast<std::byte const*>(v->data()),
      v->size(),
    };
  }

  // Visitor overloads for match/std::visit dispatch.
  auto operator()(caf::none_t) -> void {
    builder_.append_null();
  }

  auto operator()(view3<bool> x) -> void {
    builder_.append(x);
  }

  auto operator()(view3<int64_t> x) -> void {
    builder_.append(x);
  }

  auto operator()(view3<uint64_t> x) -> void {
    builder_.append(x);
  }

  auto operator()(view3<double> x) -> void {
    switch (std::fpclassify(x)) {
      case FP_NORMAL:
      case FP_SUBNORMAL:
      case FP_ZERO:
        break;
      default:
        (*this)(caf::none);
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

  auto operator()(view3<duration> x) -> void {
    if (options_.numeric_durations) {
      auto const seconds
        = std::chrono::duration_cast<std::chrono::duration<double>>(x).count();
      (*this)(seconds);
      return;
    }
    builder_.append('"');
    builder_.append_raw(fmt::format("{}", x));
    builder_.append('"');
  }

  auto operator()(view3<time> x) -> void {
    builder_.append('"');
    builder_.append_raw(fmt::format("{}", x));
    builder_.append('"');
  }

  auto operator()(view3<std::string> x) -> void {
    builder_.escape_and_append_with_quotes(x);
  }

  auto operator()(view3<blob> x) -> void {
    // Base64 output is ASCII-safe, no escaping needed.
    auto encoded = detail::base64::encode(x);
    builder_.append('"');
    builder_.append_raw(encoded);
    builder_.append('"');
  }

  auto operator()(view3<secret> x) -> void {
    auto const str = fmt::format("{}", x);
    builder_.escape_and_append_with_quotes(str);
  }

  auto operator()(view3<ip> x) -> void {
    builder_.append('"');
    builder_.append_raw(fmt::format("{}", x));
    builder_.append('"');
  }

  auto operator()(view3<subnet> x) -> void {
    builder_.append('"');
    builder_.append_raw(fmt::format("{}", x));
    builder_.append('"');
  }

  auto operator()(view3<enumeration> x) -> void {
    builder_.append(static_cast<uint64_t>(x));
  }

  auto operator()(view3<list> x) -> void {
    builder_.start_array();
    auto first = true;
    for (auto const& element : x) {
      if (should_skip(element, true)) {
        continue;
      }
      if (not first) {
        builder_.append_comma();
      }
      first = false;
      match(element, *this);
    }
    builder_.end_array();
  }

  auto operator()(view3<record> x) -> void {
    builder_.start_object();
    auto first = true;
    for (auto const& [key, value] : x) {
      if (should_skip(value, false)) {
        continue;
      }
      if (not first) {
        builder_.append_comma();
      }
      first = false;
      builder_.escape_and_append_with_quotes(key);
      builder_.append_colon();
      match(value, *this);
    }
    builder_.end_object();
  }

private:
  auto should_skip(view3<data> x, bool in_list) -> bool {
    if (in_list and options_.omit_nulls_in_lists and is<caf::none_t>(x)) {
      return true;
    }
    if (options_.omit_null_fields and is<caf::none_t>(x)) {
      return true;
    }
    if (options_.omit_empty_lists and is<view3<list>>(x)) {
      auto const& ys = as<view3<list>>(x);
      return std::all_of(ys.begin(), ys.end(), [this](view3<data> y) {
        return should_skip(y, true);
      });
    }
    if (options_.omit_empty_records and is<view3<record>>(x)) {
      auto const& ys = as<view3<record>>(x);
      return std::all_of(ys.begin(), ys.end(), [this](auto const& y) {
        return should_skip(y.second, false);
      });
    }
    return false;
  }

  simdjson::builder::string_builder builder_;
  json_printer_options options_ = {};
};

} // namespace tenzir
