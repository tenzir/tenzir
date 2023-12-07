//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/print.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/detail/logger_formatters.hpp"

#include <caf/inspector_access_type.hpp>

namespace tenzir {

/// The "default formatter" provides an `fmt::formatter` for types that might
/// have `to_string` functionality and those that can used with CAF's `inspect`.
/// Depending on the availability of both, the default formatter supports the
/// format strings `{}` (for user-friendly stringification), `{:?}` (for concise
/// debug information), and `{:#?}` (for multi-line debug information).
///
/// To enable the default formatter for a type `T`, supply a specialization of
/// `enable_default_formatter` with a `::value` member that is `true`.
template <class T>
struct enable_default_formatter : std::false_type {};

namespace detail {

template <class T>
auto stringify(fmt::format_context::iterator out, const T& x) {
  if constexpr (printable<decltype(out), T>) {
    return print(out, x);
  } else if constexpr (requires { x.to_string(); }) {
    auto str = x.to_string();
    return std::copy(str.begin(), str.end(), out);
  } else if constexpr (requires { to_string(x); }) {
    auto str = to_string(x);
    return std::copy(str.begin(), str.end(), out);
  } else {
    return;
  }
}

template <class T>
concept can_stringify
  = not std::same_as<decltype(tenzir::detail::stringify(
                       std::declval<fmt::format_context::iterator>(),
                       std::declval<T>())),
                     void>;

template <class T>
concept can_inspect = not std::same_as<
  decltype(caf::inspect_access_type<tenzir::debug_writer, T>()),
  caf::inspector_access_type::none>;

} // namespace detail

/// Utility class to use the default formatter for arbitrary types.
template <class T>
class use_default_formatter {
public:
  explicit use_default_formatter(const T& inner) : inner_{inner} {
  }

  auto to_string() const
    requires detail::can_stringify<T> || fmt::is_formattable<T>::value
  {
    if constexpr (detail::can_stringify<T>) {
      return detail::stringify(inner_);
    } else {
      return fmt::to_string(inner_);
    }
  }

  friend auto inspect(auto& f, use_default_formatter& x) -> bool
    requires detail::can_inspect<T>
  {
    return f.apply(x.inner_);
  }

private:
  const T& inner_;
};

template <class T>
struct enable_default_formatter<use_default_formatter<T>> : std::true_type {};

} // namespace tenzir

template <class T>
  requires(tenzir::enable_default_formatter<T>::value)
struct fmt::formatter<T> {
  enum { normal, debug, pretty_debug } mode = normal;

  static constexpr bool can_stringify = tenzir::detail::can_stringify<T>;

  static constexpr bool can_inspect = tenzir::detail::can_inspect<T>;

  constexpr auto parse(fmt::format_parse_context& ctx)
    -> fmt::format_parse_context::iterator {
    auto it = ctx.begin();
    if (it != ctx.end()) {
      if (*it == '?') {
        mode = debug;
        return ++it;
      } else if (*it == '#') {
        auto next = std::next(it);
        if (next != ctx.end() && *next == '?') {
          mode = pretty_debug;
          return ++next;
        }
      }
    }
    // TODO: Do we want make `{}` use `inspect` if there is no `to_string`?
    if (mode == normal) {
      if (not can_stringify && not can_inspect) {
        throw fmt::format_error{
          "the `{}` format specifier requires `to_string` or `inspect`"};
      }
    } else {
      if (not can_inspect) {
        throw fmt::format_error{
          "the `{:?}` and `{:#?}` format specifiers require `inspect`"};
      }
    }
    return it;
  }

  auto format(const T& x, fmt::format_context& ctx) const
    -> fmt::format_context::iterator {
    if constexpr (can_stringify) {
      if (mode == normal) {
        return tenzir::detail::stringify(ctx.out(), x);
      }
    }
    if constexpr (can_inspect) {
      // TODO: Consider passing `fmt::appender` to `debug_writer`.
      auto w = tenzir::debug_writer{};
      if (mode == pretty_debug) {
        w.indentation(2);
      }
      auto success = w.apply(x);
      auto out = fmt::format_to(ctx.out(), "{}", w.str());
      if (not success) {
        out = fmt::format_to(out, "<error: {}>", w.get_error());
      }
      return out;
    }
    TENZIR_UNREACHABLE();
  }
};
