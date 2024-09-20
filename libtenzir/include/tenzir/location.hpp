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
#include "tenzir/tql2/source_map.hpp"

#include <fmt/format.h>

namespace tenzir {

struct into_location;

/// Identifies a consecutive byte sequence within a source file.
///
/// If both offsets are zero, the location is unknown. Otherwise, the location
/// corresponds to the range `[begin, end)` in the main source file. In the
/// future, a `file` field might be added in order to support diagnostics from
/// multiple files simultaneously.
struct location {
  // TODO: Backwards compatibility. If not set, take source from diagnostic
  // printer.
  source_id source;
  size_t begin;
  size_t end;

  location() : source{source_id::unknown}, begin{0}, end{0} {
  }

  location(source_id source, size_t begin, size_t end)
    : source{source}, begin{begin}, end{end} {
  }

  // TODO: Remove this.
  static auto legacy(size_t begin, size_t end) -> location {
    return location{source_id::unknown, begin, end};
  }

  /// The "unknown" location, where `begin` and `end` are 0.
  static const location unknown;

  /// Returns true if the location is known, and false otherwise.
  explicit operator bool() const {
    return *this != unknown;
  }

  auto
  subloc(size_t pos, size_t count = std::numeric_limits<size_t>::max()) const
    -> location {
    if (*this == unknown || pos > end) {
      return *this;
    }
    const auto first = begin + pos;
    const auto last = (count > end - first) ? end : (first + count);
    return {source, first, last};
  }

  auto combine(into_location other) const -> location;

  auto operator<=>(const location&) const = default;

  friend auto inspect(auto& f, location& x) {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->fmt_value("{}..{}", x.begin, x.end);
    }
    return f.object(x)
      .pretty_name("location")
      .fields(f.field("source", x.source), f.field("begin", x.begin),
              f.field("end", x.end));
  }
};

inline const location location::unknown = location{};

template <>
inline constexpr auto enable_default_formatter<location> = true;

/// Provides a `T` together with a `location`.
template <class T>
struct located {
  using value_type = T;

  T inner{};
  location source;

  located() = default;

  template <typename U = T>
    requires std::is_constructible_v<T, U&&>
  located(U&& inner, location source)
    : inner(std::forward<U>(inner)), source(source) {
  }

  template <typename U>
    requires std::is_constructible_v<T, const U&>
  explicit(!std::is_convertible_v<const U&, T>) located(const located<U>& other)
    : inner(other.inner), source(other.source) {
  }

  template <typename U>
    requires std::is_constructible_v<T, U&&>
  explicit(!std::is_convertible_v<U&&, T>) located(located<U>&& other)
    : inner(std::move(other.inner)), source(other.source) {
  }

  template <typename U>
    requires(std::is_constructible_v<T, const U&>
             && std::is_assignable_v<T&, const U&>)
  auto operator=(const located<U>& other) -> located& {
    inner = other.inner;
    source = other.source;
    return *this;
  }

  template <typename U>
    requires(std::is_constructible_v<T, U> && std::is_assignable_v<T&, U>)
  auto operator=(located<U&>& other) -> located& {
    inner = std::move(other.inner);
    source = other.source;
    return *this;
  }

  auto operator<=>(const located&) const = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, located& x) {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->apply(x.inner) && dbg->append(" @ {:?}", x.source);
    }
    return f.object(x).pretty_name("located").fields(
      f.field("inner", x.inner), f.field("source", x.source));
  }
};

template <class T>
located(T, location) -> located<T>;

template <class T>
inline constexpr auto enable_default_formatter<located<T>> = true;

/// Utility type that provides implicit conversions to `location`.
struct into_location : location {
  using location::location;

  explicit(false) into_location(location x) : location{x} {
  }

  template <class T>
  explicit(false) into_location(const located<T>& x) : location{x.source} {
  }

  // TODO: Make this a customization point instead.
  template <class T>
    requires requires(const T& x) { x.get_location(); }
  explicit(false) into_location(const T& x) : location{x.get_location()} {
  }
};

inline auto location::combine(into_location other) const -> location {
  // TODO: This is dangerous.
  TENZIR_ASSERT(source == other.source);
  if (not *this) {
    return other;
  }
  if (not other) {
    return *this;
  }
  other.begin = std::min(begin, other.begin);
  other.end = std::max(end, other.end);
  return other;
}

template <class T>
struct as_located {
  using type = located<T>;
};

template <class T>
struct is_located : caf::detail::is_specialization<located, T> {};

} // namespace tenzir
