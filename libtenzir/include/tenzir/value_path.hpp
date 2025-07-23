//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/variant.hpp"

#include <fmt/format.h>

#include <string_view>

namespace tenzir {

class value_path {
public:
  value_path() = default;

  auto field(std::string_view name) -> value_path {
    return {*this, field_t{name}};
  }

  auto list() -> value_path {
    return {*this, list_t{}};
  }

  auto index(int64_t index) -> value_path {
    return {*this, index_t{index}};
  }

private:
  friend struct fmt::formatter<tenzir::value_path>;

  struct field_t {
    std::string_view name;
  };
  struct list_t {};
  struct index_t {
    int64_t value;
  };

  using segment = variant<field_t, list_t, index_t>;

  value_path(const value_path& previous, segment segment)
    : value_path{{previous, segment}} {
  }

  explicit value_path(std::pair<const value_path&, segment> data)
    : data_{data} {
  }

  std::optional<std::pair<const value_path&, segment>> data_;
};

} // namespace tenzir

template <>
struct fmt::formatter<tenzir::value_path> {
  constexpr auto parse(format_parse_context& ctx)
    -> format_parse_context::iterator {
    return ctx.begin();
  }

  static auto format(const tenzir::value_path& x, fmt::format_context& ctx)
    -> fmt::format_context::iterator {
    auto out = ctx.out();
    if (not x.data_) {
      return fmt::format_to(out, "this");
    }
    auto recurse = [&](this auto&& self, const tenzir::value_path& previous,
                       const tenzir::value_path::segment& segment) -> void {
      auto first = true;
      if (previous.data_) {
        first = false;
        self(previous.data_->first, previous.data_->second);
      }
      match(
        segment,
        [&](tenzir::value_path::field_t field) {
          if (not first) {
            *out++ = '.';
          }
          out = std::copy(field.name.begin(), field.name.end(), out);
        },
        [&](tenzir::value_path::list_t) {
          if (first) {
            out = fmt::format_to(out, "this");
          }
          out = fmt::format_to(out, "[]");
        },
        [&](tenzir::value_path::index_t index) {
          if (first) {
            out = fmt::format_to(out, "this");
          }
          out = fmt::format_to(out, "[{}]", index.value);
        });
    };
    recurse(x.data_->first, x.data_->second);
    return out;
  }
};
