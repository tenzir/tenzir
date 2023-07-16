//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/location.hpp"

namespace tenzir {

///
struct identifier {
  std::string name;
  location source;

  auto operator==(const identifier&) const -> bool = default;

  auto operator==(std::string_view other) const -> bool {
    return name == other;
  }

  friend auto inspect(auto& f, identifier& x) {
    return f.object(x).fields(f.field("name", x.name),
                              f.field("source", x.source));
  }
};

} // namespace tenzir

template <>
struct fmt::formatter<tenzir::identifier> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const tenzir::identifier& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{{name: {}, source: {}}}", x.name,
                          x.source);
  }
};
