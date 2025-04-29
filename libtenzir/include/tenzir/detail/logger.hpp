//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/detail/type_traits.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <spdlog/spdlog.h>

#include <string>
#include <string_view>
#include <type_traits>

namespace tenzir::detail {

/// Initialize the spdlog
/// Creates the log and the sinks, sets loglevels and format
/// Must be called before using the logger, otherwise log messages will
/// silently be discarded.
bool setup_spdlog(bool is_server, const tenzir::invocation& cmd_invocation,
                  const caf::settings& cfg_file);

/// Shuts down the logging system
/// Since tenzir logger runs async and has therefore a  background thread.
/// for a graceful exit this function should be called.
void shutdown_spdlog() noexcept;

/// Get a spdlog::logger handel
std::shared_ptr<spdlog::logger>& logger();

/// Checks if spdlog is already setup by checking `logger()->name()`
auto is_spdlog_setup() noexcept -> bool;

template <class T>
auto pretty_type_name(const T&) {
  return caf::detail::pretty_type_name(typeid(std::remove_pointer_t<T>));
}

template <class T>
struct single_arg_wrapper {
  const std::string_view name;
  const T& value;
};

template <class T>
single_arg_wrapper<T> make_arg_wrapper(const char* name, const T& value) {
  return {name, value};
}

template <class Iterator>
struct range_arg_wrapper {
  const std::string_view name;
  Iterator first;
  Iterator last;
};

template <class Iterator>
range_arg_wrapper<Iterator>
make_arg_wrapper(std::string_view name, Iterator first, Iterator last) {
  return {name, first, last};
}

} // namespace tenzir::detail
