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

#pragma once

#include "vast/fwd.hpp"

#include "vast/concept/printable/print.hpp"
#include "vast/detail/logger.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/detail/stringification_inspector.hpp>

// TODO: Find a way that removes the need to include these in every translation
// unit, e.g., by introducing a helper function that calls handle.name().
#include <caf/event_based_actor.hpp>
#include <caf/io/broker.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <string>
#include <type_traits>

#include <spdlog/spdlog.h>
#ifndef SPDLOG_FMT_EXTERNAL
#  include <spdlog/fmt/bundled/chrono.h>
#  include <spdlog/fmt/bundled/ostream.h>
#  include <spdlog/fmt/bundled/ranges.h>
#else
#  include <fmt/chrono.h>
#  include <fmt/ostream.h>
#  include <fmt/ranges.h>
#endif

// A fallback formatter using the `caf::detail::stringification_inspector`
// concept, which uses ADL-available `to_string` overloads if available.
template <typename T>
struct fmt::internal::fallback_formatter<
  T, fmt::format_context::char_type,
  std::enable_if_t<std::conjunction_v<
    std::negation<fmt::has_formatter<T, fmt::format_context>>,
    std::negation<vast::is_printable<std::back_insert_iterator<std::string>,
                                     std::decay_t<T>>>,
    caf::detail::is_inspectable<caf::detail::stringification_inspector, T>>>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const T& item, FormatContext& ctx) {
    auto result = std::string{};
    auto f = caf::detail::stringification_inspector{result};
    f(item);
    return format_to(ctx.out(), "{}", result);
  }
};

// A fallback formatter using VAST's printable concept.
template <typename T>
struct fmt::internal::fallback_formatter<
  T, fmt::format_context::char_type,
  std::enable_if_t<std::conjunction_v<
    std::negation<fmt::has_formatter<T, fmt::format_context>>,
    vast::is_printable<std::back_insert_iterator<std::string>, std::decay_t<T>>>>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const T& item, FormatContext& ctx) {
    auto result = std::string{};
    vast::print(std::back_inserter(result), item);
    return format_to(ctx.out(), "{}", result);
  }
};

template <class T>
struct fmt::formatter<vast::detail::single_arg_wrapper<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto
  format(const vast::detail::single_arg_wrapper<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{} = {}", item.name, item.value);
  }
};

template <class T>
struct fmt::formatter<vast::detail::range_arg_wrapper<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto
  format(const vast::detail::range_arg_wrapper<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{} = <{}>", item.name,
                     fmt::join(item.first, item.last, ", "));
  }
};

template <>
struct fmt::formatter<caf::config_value> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::config_value& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", caf::deep_to_string(item));
  }
};

template <class T>
struct fmt::formatter<caf::intrusive_ptr<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::intrusive_ptr<T>& item, FormatContext& ctx) {
    if (!item)
      return format_to(ctx.out(), "*{}", fmt::ptr(item.get()));
    return format_to(ctx.out(), "*{}", "nullptr");
  }
};

template <class T>
struct fmt::formatter<caf::intrusive_cow_ptr<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::intrusive_cow_ptr<T>& item, FormatContext& ctx) {
    if (!item)
      return format_to(ctx.out(), "*{}", fmt::ptr(item.get()));
    return format_to(ctx.out(), "*{}", "nullptr");
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

template <class... Sigs>
struct fmt::formatter<caf::typed_event_based_actor<Sigs...>*> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::typed_event_based_actor<Sigs...>* item,
              FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item->name());
  }
};

template <class T>
struct fmt::formatter<caf::optional<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::optional<T>& item, FormatContext& ctx) {
    if (!item)
      return format_to(ctx.out(), "*nullopt");
    return format_to(ctx.out(), "*{}", *item);
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
    if (!item)
      return format_to(ctx.out(), "*{}", item.error());
    return format_to(ctx.out(), "*{}", *item);
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
    return format_to(ctx.out(), "{}", vast::render(item));
  }
};

template <typename T>
struct fmt::formatter<vast::span<T>, fmt::format_context::char_type,
                      std::enable_if_t<!std::is_same_v<T, vast::byte>>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::span<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "vast.span({})",
                     fmt::join(item.begin(), item.end(), ", "));
  }
};

template <>
struct fmt::formatter<vast::span<vast::byte>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::span<vast::byte>&, FormatContext& ctx) {
    // Inentioanlly unprintable.
    return format_to(ctx.out(), "vast.span(<bytes>)");
  }
};

template <typename T>
struct fmt::formatter<caf::stream<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::stream<T>&, FormatContext& ctx) {
    return format_to(ctx.out(), "caf.stream<{}>",
                     caf::detail::pretty_type_name(typeid(T)));
  }
};

template <typename T>
struct fmt::formatter<caf::downstream<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::downstream<T>&, FormatContext& ctx) {
    return format_to(ctx.out(), "caf.downstream<{}>",
                     caf::detail::pretty_type_name(typeid(T)));
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
struct fmt::formatter<caf::scoped_actor> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::scoped_actor& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item->name());
  }
};

template <>
struct fmt::formatter<caf::actor_control_block> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::actor_control_block& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item.id());
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
    return format_to(ctx.out(), "{}", item->name());
  }
};
