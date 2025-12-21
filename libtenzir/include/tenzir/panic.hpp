//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp"

#include <boost/stacktrace/stacktrace.hpp>
#include <fmt/format.h>

#include <dlfcn.h>
#include <source_location>

namespace tenzir {

struct panic_exception final : std::exception {
  panic_exception(std::string message, std::source_location location,
                  boost::stacktrace::stacktrace stacktrace)
    : message{std::move(message)},
      location{location},
      stacktrace{std::move(stacktrace)},
      trace{} {
  }

  auto what() const noexcept -> const char* override {
    // TODO: Is this safe?
    if (what_.empty()) {
      what_ = fmt::format("{} (at {}:{})", message, location.file_name(),
                          location.column());
    }
    return what_.c_str();
  }

  std::string message;
  std::source_location location;
  boost::stacktrace::stacktrace stacktrace;
  mutable std::string what_;

  // We can't include the location.hpp header here as that'd be a circular
  // include, so we roll our own and convert it into a location upon printing.
  struct {
    size_t begin = {};
    size_t end = {};
  } trace;
};

template <class... Ts>
struct located_format_string {
  template <class String>
  explicit(false) consteval located_format_string(
    String string, std::source_location location
                   = std::source_location::current())
    : string{std::move(string)}, location{location} {
  }

  fmt::format_string<Ts...> string;
  std::source_location location;
};

template <size_t Skip = 0, class... Ts>
[[noreturn]] TENZIR_NO_INLINE void
panic_at(std::source_location location, fmt::format_string<Ts...> string,
         Ts&&... xs) {
  auto st = boost::stacktrace::stacktrace{Skip + 1, 1000};
  throw panic_exception{fmt::format(std::move(string), std::forward<Ts>(xs)...),
                        location, std::move(st)};
}

template <size_t Skip = 0, class... Ts>
[[noreturn]] TENZIR_NO_INLINE void
panic(located_format_string<std::type_identity_t<Ts>...> located_string,
      Ts&&... xs) {
  panic_at<Skip + 1>(located_string.location, std::move(located_string.string),
                     std::forward<Ts>(xs)...);
}

template <size_t Skip = 0, class T>
[[noreturn]] TENZIR_NO_INLINE void
panic(T&& message, std::source_location location
                   = std::source_location::current()) {
  panic_at<Skip + 1>(location, "{}", std::forward<T>(message));
}

} // namespace tenzir
