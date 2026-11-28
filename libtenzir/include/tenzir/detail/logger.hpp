//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/detail/type_traits.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <spdlog/spdlog.h>

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <utility>

namespace tenzir::detail {

/// Replaces the placeholder logger with one that writes to the sinks selected
/// by the command line and the configuration file. Until this succeeds, all
/// log messages are dropped.
/// @returns `false` if the configuration is invalid or the logger is already
///          set up.
bool setup_spdlog(bool is_server, const tenzir::invocation& cmd_invocation,
                  const caf::settings& cfg_file);

/// Flushes pending log messages and stops the background thread of the
/// asynchronous logger. Call this before the process exits.
void shutdown_spdlog() noexcept;

/// Returns the process-wide logger that the `TENZIR_*` logging macros use.
std::shared_ptr<spdlog::logger>& logger();

inline auto pretty_type_name(const char* n) -> std::string {
  return caf::detail::pretty_type_name(n);
}

inline auto pretty_type_name(const std::type_info& i) -> std::string {
  return pretty_type_name(i.name());
}

template <class T>
auto pretty_type_name(const T&) {
  return pretty_type_name(typeid(std::remove_pointer_t<T>));
}

/// A value labeled with a name, rendered as `name = value` in log output.
/// Created by `TENZIR_ARG`.
template <class T>
struct named_value {
  std::string_view name;
  const T& value;
};

/// A range labeled with a name, rendered as `name = <x, y, ...>` in log
/// output. Created by `TENZIR_ARG`.
template <class Iterator>
struct named_range {
  std::string_view name;
  Iterator first;
  Iterator last;
};

template <class T>
auto make_arg_wrapper(const char* name, const T& value) -> named_value<T> {
  return named_value<T>{name, value};
}

template <class Iterator>
auto make_arg_wrapper(std::string_view name, Iterator first, Iterator last)
  -> named_range<Iterator> {
  return named_range<Iterator>{name, std::move(first), std::move(last)};
}

} // namespace tenzir::detail
