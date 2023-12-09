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

template <>
inline constexpr auto enable_default_formatter<identifier> = true;

} // namespace tenzir
