//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/fwd.hpp"

#include <fmt/format.h>

namespace vast {

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
    return f.object(x)
      .pretty_name("location")
      .fields(f.field("begin", x.begin), f.field("end", x.end));
  }
};

inline const location location::unknown = location{};

/// Provides a `T` together with a `location`.
template <class T>
struct located {
  T inner{};
  location source;

  located() = default;

  located(T inner, location source) : inner{std::move(inner)}, source{source} {
  }

  auto operator<=>(const located&) const = default;

  friend auto inspect(auto& f, located& x) {
    return f.object(x).pretty_name("located").fields(
      f.field("inner", x.inner), f.field("source", x.source));
  }
};

} // namespace vast

template <>
struct fmt::formatter<vast::location> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::location& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{{begin: {}, end: {}}}", x.begin, x.end);
  }
};

template <class T>
struct fmt::formatter<vast::located<T>> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::located<T>& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{{inner: {}, source: {}}}", x.inner,
                          x.source);
  }
};
