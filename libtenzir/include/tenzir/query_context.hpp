//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/uuid.hpp"

#include <caf/typed_actor_view.hpp>

namespace tenzir {

/// An extract query to retrieve the events that match the expression.
struct extract_query_context {
  receiver_actor<table_slice> sink;

  friend bool operator==(const extract_query_context& lhs,
                         const extract_query_context& rhs) {
    return lhs.sink == rhs.sink;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, extract_query_context& x) {
    return f.object(x)
      .pretty_name("tenzir.query.extract")
      .fields(f.field("sink", x.sink));
  }
};

/// A wrapper for an expression related command.
struct query_context {
  /// The query command type.
  using command = caf::variant<extract_query_context>;

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
  make_extract(std::string issuer, const Actor& sink, expression expr) {
    return {
      std::move(issuer),
      extract_query_context{caf::actor_cast<receiver_actor<table_slice>>(sink)},
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
      .pretty_name("tenzir.query")
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
  tenzir::ids ids = {};

  struct priority {
    constexpr static uint64_t normal = 1'000;
    constexpr static uint64_t low = 1;
  };

  /// The query priority.
  uint64_t priority = priority::normal;

  /// The issuer of the query.
  std::string issuer = {};
};

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::query_context> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::query_context& value, FormatContext& ctx) const
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    auto f = tenzir::detail::overload{
      [&](const tenzir::extract_query_context&) {
        out = fmt::format_to(out, "extract(");
      },
    };
    match(value.cmd, f);
    return fmt::format_to(out, "{} (priority={}), ids={}, issuer={})",
                          value.expr, value.priority, value.ids, value.issuer);
  }
};

} // namespace fmt
