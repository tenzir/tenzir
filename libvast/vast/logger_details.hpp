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

#include "vast/bitmap.hpp"
#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/detail/pp.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/expression.hpp"
#include "vast/format/multi_layout_reader.hpp"
#include "vast/path.hpp"
#include "vast/schema.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"
#include "vast/view.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/string_view.hpp>

#include <string>
#include <type_traits>

#include <spdlog/spdlog.h>
#define FMT_SAFE_DURATION_CAST 1
#ifndef SPDLOG_FMT_EXTERNAL
#  include <spdlog/fmt/bundled/chrono.h>
#  include <spdlog/fmt/bundled/ostream.h>
#else
#  include <fmt/chrono.h>
#  include <fmt/ostream.h>
#endif

// ^^ this is wrong, actually it should be
// #  include <spdlog/fmt/chrono.h>
// #  include <spdlog/fmt/ostr.h>
// and based on SPDLOG_FMT_EXTERNAL these headers
//  will switch to the correct ones.
// but spdlog/fmt/chrono.h is not in the old version ubuntu shipps


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
std::shared_ptr<spdlog::logger> logger();

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

template <>
struct fmt::formatter<vast::path> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::path& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::type> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::type& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};
template <>
struct fmt::formatter<vast::schema> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::schema& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::uuid> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::uuid& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::record_field> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::record_field& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::predicate> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::predicate& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::bitmap> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::bitmap& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::offset> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::offset& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::format::multi_layout_reader> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto
  format(const vast::format::multi_layout_reader& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::curried_predicate> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::curried_predicate& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::invocation> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::invocation& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <typename T>
struct fmt::formatter<vast::detail::stable_set<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::detail::stable_set<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::record_type> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::record_type& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::data_view> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::data_view& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<vast::expression> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::expression& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

// ---------std types

template <typename T>
struct fmt::formatter<std::vector<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const std::vector<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<std::map<vast::offset, std::pair<size_t, vast::ids>>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const std::map<vast::offset, std::pair<size_t, vast::ids>>& item,
              FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

// ---------caf types

template <class T>
struct fmt::formatter<caf::optional<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::optional<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <class T>
struct fmt::formatter<caf::expected<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::expected<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<caf::error> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::error& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<caf::actor> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::actor& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<caf::actor_addr> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::actor_addr& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<caf::event_based_actor> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::event_based_actor& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item.name());
  }
};

template <typename... T>
struct fmt::formatter<caf::typed_actor<T...>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::typed_actor<T...>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<caf::event_based_actor*> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::event_based_actor* item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item->name());
  }
};

template <class State, class Base>
struct fmt::formatter<caf::stateful_actor<State, Base>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto
  format(const caf::stateful_actor<State, Base>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item.name());
  }
};

template <class State, class Base>
struct fmt::formatter<caf::stateful_actor<State, Base>*> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::stateful_actor<State, Base>* const item,
              FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item->name());
  }
};

template <>
struct fmt::formatter<caf::strong_actor_ptr> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::strong_actor_ptr& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <>
struct fmt::formatter<caf::io::broker*> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::io::broker* item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

// end up with too many ambigous calls, and some crashed ...?
//  but it could be evaluated it that would be possible
// activate, deactivate some above, build , test , see if it works ...

// template <class T, class Char>
// struct fmt::formatter<
//   T, Char,
//   std::enable_if_t<
//     vast::detail::has_to_string<T>
//      && fmt::detail::type_constant<T, Char>::value ==
//      fmt::detail::type::custom_type
//     //  && fmt::internal::type_constant<T, Char>::value ==
//     fmt::internal::type::custom_type
//     >> {

//   template <typename ParseContext>
//   constexpr auto parse(ParseContext& ctx) {
//     return ctx.begin();
//   }

//   template <typename FormatContext>
//   auto format(const T& x, FormatContext& ctx) {
//     return format_to(ctx.out(), "{}", to_string(x));
//   }
// };
