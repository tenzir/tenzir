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
#include "vast/detail/logger.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"
#include "vast/scope_linked.hpp"
#include "vast/uuid.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/detail/stringification_inspector.hpp>

// TODO: Find a way that removes the need to include these in every translation
// unit, e.g., by introducing a helper function that calls handle.name().
#include <caf/event_based_actor.hpp>
#include <caf/io/broker.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <spdlog/spdlog.h>

#include <fmt/chrono.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <cstddef>
#include <optional>
#include <string>
#include <type_traits>

namespace vast::detail {

#if FMT_VERSION >= 70000 // {fmt} 7+

template <typename T>
struct is_formattable
  : std::bool_constant<!std::is_same<
      decltype(fmt::internal::arg_mapper<fmt::buffer_context<char>>().map(
        std::declval<T>())),
      fmt::internal::unformattable>::value> {};

#elif FMT_VERSION >= 60000 // {fmt} 6.x

template <typename T, typename = void>
struct is_formattable : std::false_type {};

template <typename T>
struct is_formattable<
  T, std::enable_if_t<fmt::has_formatter<T, fmt::buffer_context<char>>::value>>
  : std::true_type {};

#else // {fmt} 5.x

template <typename T, typename = void>
struct is_formattable : std::false_type {};

template <typename T>
struct is_formattable<
  T, std::enable_if_t<fmt::has_formatter<T, fmt::buffer_context<char>>::value>>
  : std::true_type {};

#endif

} // namespace vast::detail

// A fallback formatter using the `caf::detail::stringification_inspector`
// concept, which uses ADL-available `to_string` overloads if available.
template <typename T>
struct fmt::internal::fallback_formatter<
  T, fmt::format_context::char_type,
  std::enable_if_t<std::conjunction_v<
    std::negation<vast::detail::is_formattable<T>>,
    std::negation<fmt::internal::is_streamable<T, fmt::format_context::char_type>>,
    std::negation<vast::is_printable<std::back_insert_iterator<std::string>,
                                     std::decay_t<T>>>,
    caf::detail::is_inspectable<caf::detail::stringification_inspector, T>>>>
  : private formatter<basic_string_view<fmt::format_context::char_type>,
                      fmt::format_context::char_type> {
  using super = formatter<basic_string_view<fmt::format_context::char_type>,
                          fmt::format_context::char_type>;
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return super::parse(ctx);
  }

  template <typename FormatContext>
  auto format(const T& item, FormatContext& ctx) -> decltype(ctx.out()) {
    auto result = std::string{};
    auto f = caf::detail::stringification_inspector{result};
    f(item);
    return super::format(result, ctx);
  }
};

// A fallback formatter using VAST's printable concept.
template <typename T>
struct fmt::internal::fallback_formatter<
  T, fmt::format_context::char_type,
  std::enable_if_t<std::conjunction_v<
    std::negation<vast::detail::is_formattable<T>>,
    std::negation<fmt::internal::is_streamable<T, fmt::format_context::char_type>>,
    vast::is_printable<std::back_insert_iterator<std::string>, std::decay_t<T>>>>>
  : private formatter<basic_string_view<fmt::format_context::char_type>,
                      fmt::format_context::char_type> {
  using super = formatter<basic_string_view<fmt::format_context::char_type>,
                          fmt::format_context::char_type>;
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return super::parse(ctx);
  }

  template <typename FormatContext>
  auto format(const T& item, FormatContext& ctx) -> decltype(ctx.out()) {
    auto result = std::string{};
    vast::print(std::back_inserter(result), item);
    return super::format(result, ctx);
  }
};

template <>
struct fmt::formatter<vast::uuid> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::uuid& x, FormatContext& ctx) {
    // e.g. 96107185-1838-48fb-906c-d1a9941ff407
    static_assert(sizeof(vast::uuid) == 16, "id format changed, please update "
                                            "formatter");
    const auto args
      = vast::span{reinterpret_cast<const unsigned char*>(x.begin()), x.size()};
    return format_to(ctx.out(), "{:02X}-{:02X}-{:02X}-{:02X}-{:02X}",
                     fmt::join(args.subspan(0, 4), ""),
                     fmt::join(args.subspan(4, 2), ""),
                     fmt::join(args.subspan(6, 2), ""),
                     fmt::join(args.subspan(8, 2), ""),
                     fmt::join(args.subspan(10, 6), ""));
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

template <class T>
struct fmt::formatter<vast::scope_linked<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::scope_linked<T>& item, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", item.get());
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
      return format_to(ctx.out(), "*{}", "nullptr");
    return format_to(ctx.out(), "*{}", fmt::ptr(item.get()));
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
      return format_to(ctx.out(), "*{}", "nullptr");
    return format_to(ctx.out(), "*{}", fmt::ptr(item.get()));
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
struct fmt::formatter<std::optional<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const std::optional<T>& item, FormatContext& ctx) {
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
                      std::enable_if_t<!std::is_same_v<T, std::byte>>> {
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
struct fmt::formatter<vast::span<std::byte>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::span<std::byte>&, FormatContext& ctx) {
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
