//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

// clang-format off
// We need to define SPDLOG_ACTIVE_LEVEL before we include spdlog.h,
// so we have to include `logger.hpp` first in case someone else is
// including this header directly without going through `tenzir/logger.hpp`
// himself.
#include "tenzir/logger.hpp"
// clang-format on

#include "tenzir/concept/printable/print.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/detail/logger.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/error.hpp"
#include "tenzir/optional.hpp"

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

namespace tenzir {

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

} // namespace tenzir

namespace fmt {

template <class T>
  requires(tenzir::use_deep_to_string_formatter<T>::value)
struct formatter<T> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  constexpr auto format(const T& value, FormatContext& ctx) const
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", caf::deep_to_string(value));
  }
};

template <class T>
  requires(tenzir::use_name_member_formatter<T>::value)
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
    static_assert(has_name_member_function
                  || tenzir::detail::always_false_v<T>);
    return fmt::format_to(ctx.out(), "{}", value.name());
  }
};

template <class T>
struct formatter<tenzir::detail::single_arg_wrapper<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::detail::single_arg_wrapper<T>& value,
              FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{} = {}", value.name, value.value);
  }
};

template <class T>
struct formatter<tenzir::detail::range_arg_wrapper<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::detail::range_arg_wrapper<T>& value,
              FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{} = <{}>", value.name,
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
      return fmt::format_to(ctx.out(), "*{}", "nullptr");
    return fmt::format_to(ctx.out(), "*{}", ptr(value.get()));
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
      return fmt::format_to(ctx.out(), "*{}", "nullptr");
    return fmt::format_to(ctx.out(), "*{}", ptr(value.get()));
  }
};

template <class T>
struct formatter<caf::expected<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::expected<T>& value, FormatContext& ctx) const {
    if (!value)
      return fmt::format_to(ctx.out(), "{}", value.error());
    return fmt::format_to(ctx.out(), "{}", *value);
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
    return fmt::format_to(ctx.out(), "{}", tenzir::render(value));
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
    return fmt::format_to(ctx.out(), "tenzir.span({})",
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
    return fmt::format_to(ctx.out(), "tenzir.span(<bytes>)");
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
    return fmt::format_to(ctx.out(), "caf.stream<{}>",
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
    return fmt::format_to(ctx.out(), "caf.downstream<{}>",
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
  constexpr auto format(const std::error_code& value, FormatContext& ctx) const
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", value.message());
  }
};

#endif

} // namespace fmt
