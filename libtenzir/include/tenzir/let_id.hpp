//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/default_formatter.hpp"

namespace tenzir {

/// Unique identifier for `let` bindings within a pipeline.
///
/// The default-constructed object represents a not-yet-bound reference.
struct let_id {
  /// The numeric index of the associated `let`. Use with care.
  uint64_t id = 0;

  /// Returns `true` if this is bound to a `let`.
  explicit operator bool() const {
    return id != 0;
  }

  auto operator<=>(const let_id&) const = default;

  friend auto inspect(auto& f, let_id& x) -> bool {
    if (x.id == 0) {
      if (auto dbg = as_debug_writer(f)) {
        return dbg->fmt_value("free");
      }
    }
    return f.apply(x.id);
  }
};

template <>
inline constexpr auto enable_default_formatter<let_id> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::let_id> {
  auto operator()(const tenzir::let_id& x) const -> size_t {
    return std::hash<decltype(x.id)>{}(x.id);
  }
};
