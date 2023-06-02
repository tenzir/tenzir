//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/location.hpp"

namespace vast {

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

} // namespace vast

template <>
struct fmt::formatter<vast::identifier> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::identifier& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{{name: {}, source: {}}}", x.name,
                          x.source);
  }
};
