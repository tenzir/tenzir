//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/type_traits.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/string_view.hpp>
#include <spdlog/spdlog.h>

#include <string>
#include <type_traits>

namespace vast::detail {

/// Initialize the spdlog
/// Creates the log and the sinks, sets loglevels and format
/// Must be called before using the logger, otherwise log messages will
/// silently be discarded.
bool setup_spdlog(const vast::invocation& cmd_invocation,
                  const caf::settings& cfg_file);

/// Shuts down the logging system
/// Since vast logger runs async and has therefore a  background thread.
/// for a graceful exit this function should be called.
void shutdown_spdlog();

/// Get a spdlog::logger handel
std::shared_ptr<spdlog::logger>& logger();

template <class T>
auto pretty_type_name(const T&) {
  if constexpr (std::is_pointer_v<T>)
    return caf::detail::pretty_type_name(typeid(std::remove_pointer_t<T>));
  else
    return caf::detail::pretty_type_name(typeid(T));
}

template <class T>
struct single_arg_wrapper {
  const char* name;
  const T& value;

  single_arg_wrapper(const char* x, const T& y) : name(x), value(y) {
    // nop
  }
};

template <class T>
single_arg_wrapper<T> make_arg_wrapper(const char* name, const T& value) {
  return {name, value};
}

template <class Iterator>
struct range_arg_wrapper {
  const char* name;
  Iterator first;
  Iterator last;

  range_arg_wrapper(const char* x, Iterator begin, Iterator end)
    : name(x), first(begin), last(end) {
    // nop
  }
};

template <class Iterator>
range_arg_wrapper<Iterator>
make_arg_wrapper(const char* name, Iterator first, Iterator last) {
  return {name, first, last};
}

} // namespace vast::detail
