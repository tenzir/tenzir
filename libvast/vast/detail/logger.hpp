/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/fwd.hpp"

#include "vast/detail/type_traits.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/string_view.hpp>

#include <string>
#include <type_traits>

#include <spdlog/spdlog.h>

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
auto id_or_name(T&& x) {
  static_assert(!std::is_same_v<const char*, std::remove_reference<T>>,
                "const char* is not allowed for the first argument in a "
                "logging statement. Supply a component or use "
                "VAST_[ERROR|WARNING|INFO|VERBOSE|DEBUG]_ANON instead.");
  if constexpr (std::is_pointer_v<T>) {
    using value_type = std::remove_pointer_t<T>;
    if constexpr (has_ostream_operator<value_type>)
      return *x;
    else if constexpr (has_to_string<value_type>)
      return to_string(*x);
    else
      return caf::detail::pretty_type_name(typeid(value_type));
  } else {
    if constexpr (has_ostream_operator<T>)
      return std::forward<T>(x);
    else if constexpr (has_to_string<T>)
      return to_string(std::forward<T>(x));
    else
      return caf::detail::pretty_type_name(typeid(T));
  }
}

template <size_t S>
struct carrier {
  char name[S] = {0};
  constexpr const char* str() const {
    return &name[0];
    ;
  }
};
template <typename... T>
constexpr auto spd_msg_from_args(T&&...) {
  constexpr auto cnt = sizeof...(T);
  static_assert(cnt > 0);
  constexpr auto len = cnt * 3;
  carrier<len> cr{};
  for (size_t i = 0; i < cnt; ++i) {
    cr.name[i * 3] = '{';
    cr.name[i * 3 + 1] = '}';
    cr.name[i * 3 + 2] = ' ';
  }
  cr.name[len - 1] = 0;
  return cr;
}

template <class T>
struct single_arg_wrapper {
  const char* name;
  const T& value;
  single_arg_wrapper(const char* x, const T& y) : name(x), value(y) {
    // nop
  }
  template <typename OStream>
  friend OStream& operator<<(OStream& os, const single_arg_wrapper& self) {
    std::string result = self.name;
    result += " = ";
    result += caf::deep_to_string(self.value);
    ;
    return os << result;
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
  template <typename OStream>
  friend OStream& operator<<(OStream& os, const range_arg_wrapper& self) {
    std::string result = self.name;
    result += " = ";
    result += caf::deep_to_string(self);
    return os << result;
  }
};

template <class Iterator>
range_arg_wrapper<Iterator>
make_arg_wrapper(const char* name, Iterator first, Iterator last) {
  return {name, first, last};
}

} // namespace vast::detail
