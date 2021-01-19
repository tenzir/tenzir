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
#include "vast/data.hpp"
#include "vast/detail/pp.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/expression.hpp"
#include "vast/format/multi_layout_reader.hpp"
#include "vast/path.hpp"
#include "vast/schema.hpp"
#include "vast/system/accountant_actor.hpp"
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
struct fmt::formatter<vast::record> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::record& item, FormatContext& ctx) {
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
struct fmt::formatter<vast::data> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::data& item, FormatContext& ctx) {
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

// template <class... Sigs>
// class typed_event_based_actor
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

template <typename T>
struct fmt::formatter<caf::stream<T>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const caf::stream<T>& item, FormatContext& ctx) {
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
