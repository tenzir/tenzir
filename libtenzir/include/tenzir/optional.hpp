//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

#include <optional>

namespace tenzir {

template <typename T>
std::optional<T> to_optional(const T* ptr) {
  if (ptr)
    return *ptr;
  return std::nullopt;
}

} // namespace tenzir

namespace fmt {
template <class T>
struct formatter<std::optional<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const std::optional<T>& value, FormatContext& ctx) const {
    if (!value) {
      return fmt::format_to(ctx.out(), "none");
    }
    return fmt::format_to(ctx.out(), "{}", *value);
  }
};
} // namespace fmt
