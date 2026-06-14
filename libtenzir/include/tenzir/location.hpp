//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/arc.hpp"
#include "tenzir/box.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/option.hpp"

#include <fmt/format.h>

#include <functional>
#include <limits>
#include <span>
#include <utility>

namespace tenzir {

struct into_location;

/// Identifies a source in a `SourceMap`.
using SourceId = uint32_t;

/// Identifies an entry in `SourceMap::call_sites()`, where `0` means
/// top-level. Hence, valid call site ids start at `1`.
using CallSiteId = uint32_t;

/// Identifies a consecutive byte sequence within a source file.
///
/// If all fields are zero, the location is unknown. Otherwise, the location
/// corresponds to the range `[begin, end)` in the source file identified by
/// `source_index`.
struct location {
  uint32_t begin{};
  uint32_t end{};
  /// The global index of the source file the location comes from. This is
  /// populated by the parser when producing the AST.
  SourceId source_index{0};
  /// The global index of the callsite this came from; `0` means this was top
  /// level. This is populated by the compiler when going from AST -> IR.
  CallSiteId callsite_index{0};

  /// The "unknown" location, where all fields are 0.
  static const location unknown;

  /// Returns true if the location is known, and false otherwise.
  explicit operator bool() const {
    return *this != unknown;
  }

  auto subloc(uint32_t pos, uint32_t count
                            = std::numeric_limits<uint32_t>::max()) const
    -> location {
    if (*this == unknown or pos > end) {
      return *this;
    }
    const auto first = begin + pos;
    const auto last = (count > end - first) ? end : (first + count);
    return {first, last, source_index, callsite_index};
  }

  auto combine(into_location other) const -> location;

  auto operator<=>(const location&) const = default;

  friend auto inspect(auto& f, location& x) {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->fmt_value("[{}]:{}..{} from {}", x.source_index, x.begin,
                            x.end, x.callsite_index);
    }
    return f.object(x)
      .pretty_name("location")
      .fields(f.field("begin", x.begin), f.field("end", x.end),
              f.field("source_index", x.source_index),
              f.field("callsite_index", x.callsite_index));
  }
};

inline const location location::unknown = location{};

template <>
inline constexpr auto enable_default_formatter<location> = true;

/// Maps locations in compiled IR back to their originating sources.
///
/// The source map is populated during compilation from AST to IR. It records
/// all sources that took part in the compilation, as well as all call sites
/// of user-defined operators whose bodies were expanded into the result.
class SourceMap {
public:
  SourceMap();
  SourceMap(const SourceMap&);
  SourceMap(SourceMap&&) noexcept;
  auto operator=(const SourceMap&) -> SourceMap&;
  auto operator=(SourceMap&&) noexcept -> SourceMap&;
  ~SourceMap();

  /// Register a source. It will be kept alive by the SourceMap.
  void add_source(Arc<const Source> source);

  /// Register the location of a user-defined operator invocation and return
  /// its id.
  ///
  /// The location's `source_index` identifies the caller's source, and its
  /// `callsite_index` the parent call site for nested calls (`0` means
  /// top-level).
  auto add_call_site(location call_site) -> CallSiteId;

  /// Return the source for the given id.
  auto source(SourceId id) const -> Option<const Source&>;

  /// Add call-site annotations to a diagnostic.
  auto enrich(diagnostic diag) const -> diagnostic;

  /// Return the call site for the given id, which must not be `0`.
  auto call_site(CallSiteId id) const -> Option<location>;

  /// Return all registered sources.
  auto sources() const -> std::span<const Arc<const Source>>;

  /// Return all registered call sites.
  auto call_sites() const -> std::span<const location>;

private:
  struct Impl;

  Box<Impl> impl_;
};

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
  explicit(not std::is_convertible_v<const U&, T>)
    located(const located<U>& other)
    : inner(other.inner), source(other.source) {
  }

  template <typename U>
    requires std::is_constructible_v<T, U&&>
  explicit(not std::is_convertible_v<U&&, T>) located(located<U>&& other)
    : inner(std::move(other.inner)), source(other.source) {
  }

  template <typename U>
    requires(std::is_constructible_v<T, const U&>
             and std::is_assignable_v<T&, const U&>)
  auto operator=(const located<U>& other) -> located& {
    inner = other.inner;
    source = other.source;
    return *this;
  }

  template <typename U>
    requires(std::is_constructible_v<T, U> and std::is_assignable_v<T&, U>)
  auto operator=(located<U&>& other) -> located& {
    inner = std::move(other.inner);
    source = other.source;
    return *this;
  }

  auto operator<=>(const located&) const = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, located& x) {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->apply(x.inner) and dbg->append(" @ {:?}", x.source);
    }
    return f.object(x).pretty_name("located").fields(
      f.field("inner", x.inner), f.field("source", x.source));
  }
};

template <class T>
located(T, location) -> located<T>;

template <class T>
inline constexpr auto enable_default_formatter<located<T>> = true;

template <class T>
concept has_get_location = requires(const T& x) { x.get_location(); };

/// Utility type that provides implicit conversions to `location`.
struct into_location : location {
  using location::location;

  explicit(false) into_location(location x) : location{x} {
  }

  template <class T>
  explicit(false) into_location(const located<T>& x) : location{x.source} {
  }

  // TODO: Make this a customization point instead.
  template <has_get_location T>
  explicit(false) into_location(const T& x) : location{x.get_location()} {
  }

  template <has_get_location... Ts>
  explicit(false) into_location(const variant<Ts...>& x)
    : location{x.match([](auto& y) {
        return y.get_location();
      })} {
  }
};

template <class T>
struct as_located {
  using type = located<T>;
};

template <class T>
struct is_located : detail::is_specialization_of<located, T> {};

template <class TraceFn, class Fun>
auto trace_panic_impl(TraceFn&& trace_fn, Fun&& fun) -> decltype(auto) {
  try {
    return std::invoke(std::forward<decltype(fun)>(fun));
  } catch (panic_exception& panic) {
    auto trace = into_location{std::invoke(std::forward<TraceFn>(trace_fn))};
    if (trace != location::unknown
        and panic.trace.begin == location::unknown.begin
        and panic.trace.end == location::unknown.end) {
      panic.trace.begin = trace.begin;
      panic.trace.end = trace.end;
    }
    throw std::move(panic);
  }
}

template <class Trace, class Fun>
  requires(not std::is_invocable_v<Trace&>)
auto trace_panic(Trace&& trace, Fun&& fun) -> decltype(auto) {
  return trace_panic_impl(
    [&]() -> decltype(auto) {
      return std::forward<Trace>(trace);
    },
    std::forward<Fun>(fun));
}

template <class TraceFn, class Fun>
  requires(
    std::is_invocable_v<TraceFn&>
    and std::is_constructible_v<into_location, std::invoke_result_t<TraceFn&>>)
auto trace_panic(TraceFn&& trace_fn, Fun&& fun) -> decltype(auto) {
  return trace_panic_impl(std::forward<TraceFn>(trace_fn),
                          std::forward<Fun>(fun));
}

} // namespace tenzir
