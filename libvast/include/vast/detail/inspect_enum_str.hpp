//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <fmt/format.h>

namespace vast::detail {

template <class Inspector, class Enum>
  requires std::is_enum_v<Enum>
auto inspect_enum_str(Inspector& f, Enum& x,
                      std::initializer_list<std::string_view> strings) -> bool {
  using underlying_type = std::underlying_type_t<Enum>;
  if (!f.has_human_readable_format()) {
    return inspect_enum(f, x);
  }
  if constexpr (Inspector::is_loading) {
    auto y = std::string{};
    if (!f.apply(y)) {
      return false;
    }
    for (auto&& z : strings) {
      if (y == z) {
        x = static_cast<Enum>(&z - strings.begin());
        return true;
      }
    }
    f.set_error(
      caf::make_error(ec::serialization_error,
                      fmt::format("could not resolve `{}` for enum `{}`", y,
                                  detail::pretty_type_name(x))));
    return false;
  } else {
    auto index = static_cast<underlying_type>(x);
    auto size = detail::narrow<underlying_type>(strings.size());
    if (!(0 <= index && index < size)) {
      f.set_error(
        caf::make_error(ec::serialization_error,
                        fmt::format("index `{}` is out of bounds for enum `{}`",
                                    index, detail::pretty_type_name(x))));
      return false;
    }
    return f.value(strings.begin()[index]);
  }
}

} // namespace vast::detail
