//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

// clang-format off
// We need to define SPDLOG_ACTIVE_LEVEL before we include spdlog.h,
// so we have to include `logger.hpp` first in case someone else is
// including this header directly without going through `vast/logger.hpp`
// himself.
#include "vast/logger.hpp"
// clang-format on

#include "vast/concept/printable/print.hpp"
#include "vast/concepts.hpp"
#include "vast/detail/logger.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"

#include <boost/core/detail/string_view.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <fmt/chrono.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <spdlog/spdlog.h>

#include <cstddef>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <type_traits>

#if __has_include(<fmt/std.h>)
#  include <fmt/std.h>
#endif

namespace vast {

// -- use_deep_to_string_formatter --------------------------------------------

template <class T>
struct use_deep_to_string_formatter : std::false_type {};

template <class T>
  requires(T::use_deep_to_string_formatter == true)
struct use_deep_to_string_formatter<T> : std::true_type {};

template <>
struct use_deep_to_string_formatter<caf::config_value> : std::true_type {};

template <>
struct use_deep_to_string_formatter<caf::actor> : std::true_type {};

template <>
struct use_deep_to_string_formatter<caf::actor_addr> : std::true_type {};

template <>
struct use_deep_to_string_formatter<caf::actor_control_block> : std::true_type {
};

template <class... Sigs>
struct use_deep_to_string_formatter<caf::typed_actor<Sigs...>>
  : std::true_type {};

template <>
struct use_deep_to_string_formatter<caf::io::broker> : std::true_type {};

// -- use_name_member_formatter -----------------------------------------------

template <class T>
struct use_name_member_formatter : std::false_type {};

template <class T>
  requires(T::use_name_member_formatter == true)
struct use_name_member_formatter<T> : std::true_type {};

template <class State, class Base>
struct use_name_member_formatter<caf::stateful_actor<State, Base>>
  : std::true_type {};

template <class... Sigs>
struct use_name_member_formatter<caf::typed_event_based_actor<Sigs...>>
  : std::true_type {};

template <>
struct use_name_member_formatter<caf::blocking_actor> : std::true_type {};

template <>
struct use_name_member_formatter<caf::event_based_actor> : std::true_type {};

} // namespace vast

namespace fmt {

template <class T>
  requires(vast::use_deep_to_string_formatter<T>::value)
struct formatter<T> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  constexpr auto format(const T& value, FormatContext& ctx) const
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(value));
  }
};

template <class T>
  requires(vast::use_name_member_formatter<T>::value)
struct formatter<T> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  constexpr auto format(const T& value, FormatContext& ctx) const
    -> decltype(ctx.out()) {
    constexpr bool has_name_member_function = requires {
      { value.name() } -> std::convertible_to<std::string_view>;
    };
    static_assert(has_name_member_function || vast::detail::always_false_v<T>);
    return format_to(ctx.out(), "{}-{}", value.name(), value.id());
  }
};

template <class T>
struct formatter<vast::detail::single_arg_wrapper<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::detail::single_arg_wrapper<T>& value,
              FormatContext& ctx) const {
    return format_to(ctx.out(), "{} = {}", value.name, value.value);
  }
};

template <class T>
struct formatter<vast::detail::range_arg_wrapper<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::detail::range_arg_wrapper<T>& value,
              FormatContext& ctx) const {
    return format_to(ctx.out(), "{} = <{}>", value.name,
                     join(value.first, value.last, ", "));
  }
};

template <class T>
struct formatter<caf::intrusive_ptr<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::intrusive_ptr<T>& value, FormatContext& ctx) const {
    if (!value)
      return format_to(ctx.out(), "*{}", "nullptr");
    return format_to(ctx.out(), "*{}", ptr(value.get()));
  }
};

template <class T>
struct formatter<caf::intrusive_cow_ptr<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto
  format(const caf::intrusive_cow_ptr<T>& value, FormatContext& ctx) const {
    if (!value)
      return format_to(ctx.out(), "*{}", "nullptr");
    return format_to(ctx.out(), "*{}", ptr(value.get()));
  }
};

template <class T>
struct formatter<caf::optional<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::optional<T>& value, FormatContext& ctx) const {
    if (!value)
      return format_to(ctx.out(), "none");
    return format_to(ctx.out(), "{}", *value);
  }
};

#if FMT_VERSION / 10000 < 10

template <class T>
struct formatter<std::optional<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const std::optional<T>& value, FormatContext& ctx) const {
    if (!value)
      return format_to(ctx.out(), "nullopt");
    return format_to(ctx.out(), "{}", *value);
  }
};

#endif

template <class T>
struct formatter<caf::expected<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::expected<T>& value, FormatContext& ctx) const {
    if (!value)
      return format_to(ctx.out(), "{}", value.error());
    return format_to(ctx.out(), "{}", *value);
  }
};

template <class T>
struct formatter<caf::inbound_stream_slot<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto
  format(const caf::inbound_stream_slot<T>& value, FormatContext& ctx) const {
    return format_to(ctx.out(), "in:{}", value.value());
  }
};

template <class T>
struct formatter<caf::outbound_stream_slot<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto
  format(const caf::outbound_stream_slot<T>& value, FormatContext& ctx) const {
    return format_to(ctx.out(), "out:{}", value.value());
  }
};

template <>
struct formatter<caf::error> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::error& value, FormatContext& ctx) const {
    return format_to(ctx.out(), "{}", vast::render(value));
  }
};

template <>
struct formatter<boost::core::string_view> : formatter<std::string_view> {
  template <class FormatContext>
  constexpr auto format(boost::core::string_view value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return formatter<std::string_view>::format(std::string_view{value}, ctx);
  }
};

template <class T>
struct formatter<std::span<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const std::span<T>& value, FormatContext& ctx) const {
    return format_to(ctx.out(), "vast.span({})",
                     join(value.begin(), value.end(), ", "));
  }
};

template <>
struct formatter<std::span<std::byte>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const std::span<std::byte>&, FormatContext& ctx) const {
    // Inentioanlly unprintable.
    return format_to(ctx.out(), "vast.span(<bytes>)");
  }
};

template <class T>
struct formatter<caf::stream<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::stream<T>&, FormatContext& ctx) const {
    return format_to(ctx.out(), "caf.stream<{}>",
                     caf::detail::pretty_type_name(typeid(T)));
  }
};

template <class T>
struct formatter<caf::downstream<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::downstream<T>&, FormatContext& ctx) const {
    return format_to(ctx.out(), "caf.downstream<{}>",
                     caf::detail::pretty_type_name(typeid(T)));
  }
};

#if !__has_include(<fmt/std.h>)

template <>
struct formatter<std::filesystem::path> : formatter<std::string> {};

#else

template <>
struct formatter<std::error_code> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  constexpr auto format(const std::error_code& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}", value.message());
  }
};

#endif

} // namespace fmt
