//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/detail/default_formatter.hpp"

#include <fmt/format.h>

namespace tenzir {

/// Identifies a consecutive byte sequence within a source file.
///
/// If both offsets are zero, the location is unknown. Otherwise, the location
/// corresponds to the range `[begin, end)` in the main source file. In the
/// future, a `file` field might be added in order to support diagnostics from
/// multiple files simultaneously.
struct location {
  size_t begin{};
  size_t end{};

  /// The "unknown" location, where `begin` and `end` are 0.
  static const location unknown;

  /// Returns true if the location is known, and false otherwise.
  explicit operator bool() const {
    return *this != unknown;
  }

  auto operator<=>(const location&) const = default;

  friend auto inspect(auto& f, location& x) {
    if (auto dbg = as_debug_writer(f)) {
      dbg->add_fmt("{}..{}", x.begin, x.end);
      return true;
    }
    return f.object(x)
      .pretty_name("location")
      .fields(f.field("begin", x.begin), f.field("end", x.end));
  }
};

inline const location location::unknown = location{};

template <>
struct enable_default_formatter<location> : std::true_type {};

/// Provides a `T` together with a `location`.
template <class T>
struct located {
  T inner{};
  location source;

  located() = default;

  located(T inner, location source) : inner{std::move(inner)}, source{source} {
  }

  auto operator<=>(const located&) const = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, located& x) {
    if (auto dbg = as_debug_writer(f)) {
      if (not dbg->apply(x.inner)) {
        return false;
      }
      dbg->add_fmt(" @ {:?}", x.source);
      return true;
    }
    return f.object(x).pretty_name("located").fields(
      f.field("inner", x.inner), f.field("source", x.source));
  }
};

template <class T>
struct enable_default_formatter<located<T>> : std::true_type {};

} // namespace tenzir
