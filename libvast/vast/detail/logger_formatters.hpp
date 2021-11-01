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

#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/detail/stringification_inspector.hpp>

#include <filesystem>
#include <span>

// TODO: Find a way that removes the need to include these in every translation
// unit, e.g., by introducing a helper function that calls handle.name().
#include <caf/event_based_actor.hpp>
#include <caf/io/broker.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <fmt/chrono.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <spdlog/spdlog.h>

#include <cstddef>
#include <optional>
#include <string>
#include <type_traits>

namespace fmt {

template <class T>
struct formatter<vast::detail::single_arg_wrapper<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto
  format(const vast::detail::single_arg_wrapper<T>& value, FormatContext& ctx) {
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
  auto
  format(const vast::detail::range_arg_wrapper<T>& value, FormatContext& ctx) {
    return format_to(ctx.out(), "{} = <{}>", value.name,
                     join(value.first, value.last, ", "));
  }
};

template <>
struct formatter<caf::config_value> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::config_value& value, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(value));
  }
};

template <class T>
struct formatter<caf::intrusive_ptr<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::intrusive_ptr<T>& value, FormatContext& ctx) {
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
  auto format(const caf::intrusive_cow_ptr<T>& value, FormatContext& ctx) {
    if (!value)
      return format_to(ctx.out(), "*{}", "nullptr");
    return format_to(ctx.out(), "*{}", ptr(value.get()));
  }
};

template <>
struct formatter<caf::actor> : formatter<std::string> {
  template <class FormatContext>
  auto format(const caf::actor& value, FormatContext& ctx) {
    return formatter<std::string>::format(caf::deep_to_string(value), ctx);
  }
};

template <>
struct formatter<caf::actor_addr> : formatter<std::string> {
  template <class FormatContext>
  auto format(const caf::actor_addr& value, FormatContext& ctx) {
    return formatter<std::string>::format(caf::deep_to_string(value), ctx);
  }
};

template <class T>
struct formatter<caf::optional<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::optional<T>& value, FormatContext& ctx) {
    if (!value)
      return format_to(ctx.out(), "none");
    return format_to(ctx.out(), "{}", *value);
  }
};

template <class T>
struct formatter<std::optional<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const std::optional<T>& value, FormatContext& ctx) {
    if (!value)
      return format_to(ctx.out(), "nullopt");
    return format_to(ctx.out(), "{}", *value);
  }
};

template <class T>
struct formatter<caf::expected<T>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::expected<T>& value, FormatContext& ctx) {
    if (!value)
      return format_to(ctx.out(), "{}", value.error());
    return format_to(ctx.out(), "{}", *value);
  }
};

template <>
struct formatter<caf::error> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::error& value, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", vast::render(value));
  }
};

template <class T>
struct formatter<std::span<T>, format_context::char_type,
                 std::enable_if_t<vast::concepts::different<T, std::byte>>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const std::span<T>& value, FormatContext& ctx) {
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
  auto format(const std::span<std::byte>&, FormatContext& ctx) {
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
  auto format(const caf::stream<T>&, FormatContext& ctx) {
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
  auto format(const caf::downstream<T>&, FormatContext& ctx) {
    return format_to(ctx.out(), "caf.downstream<{}>",
                     caf::detail::pretty_type_name(typeid(T)));
  }
};

template <class Actor>
  requires(caf::is_actor_handle<Actor>::value)
struct formatter<Actor> : formatter<std::string> {
  template <class FormatContext>
  auto format(const Actor& value, FormatContext& ctx) {
    return formatter<std::string>::format(caf::deep_to_string(value), ctx);
  }
};

template <>
struct formatter<caf::blocking_actor> : formatter<std::string_view> {
  template <class FormatContext>
  auto format(const caf::blocking_actor& value, FormatContext& ctx) {
    return formatter<std::string_view>::format(value.name(), ctx);
  }
};

template <>
struct formatter<caf::event_based_actor> : formatter<std::string_view> {
  template <class FormatContext>
  auto format(const caf::event_based_actor& value, FormatContext& ctx) {
    return formatter<std::string_view>::format(value.name(), ctx);
  }
};

template <class... Sigs>
struct formatter<caf::typed_event_based_actor<Sigs...>>
  : formatter<std::string_view> {
  template <class FormatContext>
  auto format(const caf::typed_event_based_actor<Sigs...>& value,
              FormatContext& ctx) {
    return formatter<std::string_view>::format(value.name(), ctx);
  }
};

template <class State, class Base>
struct formatter<caf::stateful_actor<State, Base>>
  : formatter<std::string_view> {
  template <class FormatContext>
  auto
  format(const caf::stateful_actor<State, Base>& value, FormatContext& ctx) {
    return formatter<std::string_view>::format(value.name(), ctx);
  }
};

template <>
struct formatter<caf::actor_control_block> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const caf::actor_control_block& value, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", value.id());
  }
};

template <>
struct formatter<caf::io::broker> : formatter<std::string_view> {
  template <class FormatContext>
  auto format(const caf::io::broker& value, FormatContext& ctx) {
    return formatter<std::string_view>::format(value.name(), ctx);
  }
};

template <>
struct formatter<std::filesystem::path> : formatter<std::string> {};

} // namespace fmt
