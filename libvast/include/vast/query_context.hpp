//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/bitmap.hpp"
#include "vast/detail/inspection_common.hpp"
#include "vast/detail/overload.hpp"
#include "vast/expression.hpp"
#include "vast/system/actors.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_actor_view.hpp>

namespace vast {

/// A count query to collect the number of hits for the expression.
struct count_query_context {
  enum mode { estimate, exact };

  friend bool
  operator==(const count_query_context& lhs, const count_query_context& rhs) {
    return lhs.sink == rhs.sink && lhs.mode == rhs.mode;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, mode& x) {
    return detail::inspect_enum(f, x);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, count_query_context& x) {
    return f.object(x)
      .pretty_name("vast.query.count")
      .fields(f.field("sink", x.sink), f.field("mode", x.mode));
  }

  system::receiver_actor<uint64_t> sink;
  enum mode mode = {};
};

/// An extract query to retrieve the events that match the expression.
struct extract_query_context {
  system::receiver_actor<table_slice> sink;

  friend bool operator==(const extract_query_context& lhs,
                         const extract_query_context& rhs) {
    return lhs.sink == rhs.sink;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, extract_query_context& x) {
    return f.object(x)
      .pretty_name("vast.query.extract")
      .fields(f.field("sink", x.sink));
  }
};

/// A wrapper for an expression related command.
struct query_context {
  /// The query command type.
  using command = caf::variant<count_query_context, extract_query_context>;

  // -- constructor & destructor -----------------------------------------------

  query_context() = default;
  query_context(query_context&&) noexcept = default;
  query_context(const query_context&) = default;

  query_context(std::string issuer, command cmd, expression expr)
    : cmd(std::move(cmd)), expr(std::move(expr)), issuer{std::move(issuer)} {
  }

  query_context& operator=(query_context&&) noexcept = default;
  query_context& operator=(const query_context&) = default;

  ~query_context() noexcept = default;

  // -- helper functions to make query creation less boiler-platey -------------

  template <class Actor>
  static query_context
  make_count(std::string issuer, const Actor& sink,
             enum count_query_context::mode m, expression expr) {
    return {
      std::move(issuer),
      count_query_context{
        caf::actor_cast<system::receiver_actor<uint64_t>>(sink), m},
      std::move(expr),
    };
  }

  template <class Actor>
  static query_context
  make_extract(std::string issuer, const Actor& sink, expression expr) {
    return {
      std::move(issuer),
      extract_query_context{
        caf::actor_cast<system::receiver_actor<table_slice>>(sink)},
      std::move(expr),
    };
  }

  // -- misc -------------------------------------------------------------------

  friend bool operator==(const query_context& lhs, const query_context& rhs) {
    return lhs.cmd == rhs.cmd && lhs.expr == rhs.expr
           && lhs.priority == rhs.priority;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, query_context& q) {
    return f.object(q)
      .pretty_name("vast.query")
      .fields(f.field("id", q.id), f.field("cmd", q.cmd),
              f.field("expr", q.expr), f.field("ids", q.ids),
              f.field("priority", q.priority), f.field("issuer", q.issuer));
  }

  std::size_t memusage() const {
    return sizeof(*this) + ids.memusage();
  }

  // -- data members -----------------------------------------------------------

  /// The query id.
  uuid id = uuid::null();

  /// The query command.
  command cmd;

  /// The query expression.
  expression expr = {};

  /// The initial taste size.
  std::optional<uint32_t> taste = std::nullopt;

  /// The event ids to restrict the query evaluation to, if set.
  vast::ids ids = {};

  struct priority {
    constexpr static uint8_t high = 100;
    constexpr static uint8_t normal = 25;
    constexpr static uint8_t low = 1;
  };

  /// The query priority.
  uint8_t priority = priority::normal;

  /// The issuer of the query.
  std::string issuer = {};
};

} // namespace vast

namespace fmt {

template <>
struct formatter<vast::query_context> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::query_context& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    auto f = vast::detail::overload{
      [&](const vast::count_query_context& cmd) {
        out = format_to(out, "count(");
        switch (cmd.mode) {
          case vast::count_query_context::estimate:
            out = format_to(out, "estimate, ");
            break;
          case vast::count_query_context::exact:
            out = format_to(out, "exact, ");
            break;
        }
      },
      [&](const vast::extract_query_context&) {
        out = format_to(out, "extract(");
      },
    };
    caf::visit(f, value.cmd);
    return format_to(out, "{} (priority={}), ids={}, issuer={})", value.expr,
                     value.priority, value.ids, value.issuer);
  }
};

} // namespace fmt
