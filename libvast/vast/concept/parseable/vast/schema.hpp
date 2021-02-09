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

#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/type.hpp"

namespace vast {

using symbol_table = std::unordered_map<std::string, type>;

struct symbol_resolver {
  caf::expected<type> lookup(const std::string& key) {
    // First we check if the key is already locally resolved.
    auto lsym = local.find(key);
    if (lsym != local.end())
      return lsym->second;
    // Then we check if it is an unresolved local type.
    auto next = working.find(key);
    if (next != working.end())
      return resolve(next);
    // Finally, we look into the global types, This is in last place because
    // they have lower precedence, i.e. local definitions are allowed to
    // shadow global ones.
    auto gsym = global.find(key);
    if (gsym != global.end())
      return gsym->second;
    return caf::make_error(ec::parse_error, "undefined symbol:", key);
  }

  template <class Type>
  caf::expected<type> operator()(Type x) {
    return std::move(x);
  }

  caf::expected<type> operator()(const none_type& x) {
    VAST_ASSERT(!x.name().empty());
    auto concrete = lookup(x.name());
    if (!concrete)
      return concrete.error();
    return concrete->update_attributes(x.attributes());
  }

  caf::expected<type> operator()(alias_type x) {
    auto y = caf::visit(*this, x.value_type);
    if (!y)
      return y.error();
    x.value_type = *y;
    return std::move(x);
  }

  caf::expected<type> operator()(list_type x) {
    auto y = caf::visit(*this, x.value_type);
    if (!y)
      return y.error();
    x.value_type = *y;
    return std::move(x);
  }

  caf::expected<type> operator()(map_type x) {
    auto y = caf::visit(*this, x.value_type);
    if (!y)
      return y.error();
    x.value_type = *y;
    auto z = caf::visit(*this, x.key_type);
    if (!z)
      return z.error();
    x.key_type = *z;
    return std::move(x);
  }

  caf::expected<type> operator()(record_type x) {
    for (auto& [field_name, field_type] : x.fields) {
      auto y = caf::visit(*this, field_type);
      if (!y)
        return y.error();
      field_type = *y;
    }
    return std::move(x);
  }

  caf::expected<type> resolve(symbol_table::iterator next) {
    auto value = std::move(*next);
    if (local.find(value.first) != local.end())
      return caf::make_error(ec::parse_error, "duplicate definition of",
                             value.first);
    working.erase(next);
    auto x = caf::visit(*this, value.second);
    if (!x)
      return x.error();
    auto [iter, inserted] = local.emplace(value.first, std::move(*x));
    if (!inserted)
      return caf::make_error(ec::parse_error, "failed to extend local "
                                              "symbols");
    auto added = sch.add(iter->second);
    if (!added)
      return caf::make_error(ec::parse_error, "failed to insert type",
                             value.first);
    return iter->second;
  }

  caf::expected<schema> resolve() {
    while (!working.empty())
      if (auto x = resolve(working.begin()); !x)
        return x.error();
    return sch;
  }

  const symbol_table& global;
  symbol_table working;
  symbol_table local = {};
  schema sch = {};
};

struct symbol_table_parser : parser<symbol_table_parser> {
  using attribute = symbol_table;

  static constexpr auto id = type_parser::id;
  static constexpr auto skp = type_parser::skp;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& out) const {
    static_assert(detail::is_any_v<Attribute, attribute, unused_type>);
    bool duplicate_symbol = false;
    auto to_type = [&](std::tuple<std::string, type> t) -> type {
      auto [name, ty] = std::move(t);
      // If the type has already a name, we're dealing with a symbol and have
      // to create an alias.
      if (!ty.name().empty())
        ty = alias_type{ty}; // TODO: attributes
      ty.name(name);
      duplicate_symbol = !out.emplace(name, ty).second;
      if (duplicate_symbol)
        VAST_ERROR("multiple definitions of {} detected", name);
      return ty;
    };
    // We can't use & because the operand is a parser, and our DSL overloads &.
    auto tp = parsers::type;
    // clang-format off
    auto decl = ("type" >> skp >> id >> skp >> '=' >> skp >> tp) ->* to_type;
    // clang-format on
    auto declarations = +(skp >> decl) >> skp;
    if (!declarations(f, l, unused))
      return false;
    if (duplicate_symbol)
      return false;
    return true;
  }
};

template <>
struct parser_registry<symbol_table> {
  using type = symbol_table_parser;
};

namespace parsers {

constexpr auto symbol_table = symbol_table_parser{};

} // namespace parsers

struct schema_parser : parser<schema_parser> {
  using attribute = schema;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& out) const {
    symbol_table global;
    symbol_table working;
    auto p = symbol_table_parser{};
    if (!p(f, l, working))
      return false;
    auto r = symbol_resolver{global, std::move(working)};
    auto sch = r.resolve();
    if (!sch) {
      VAST_WARN("failed to resolve symbol table: {}", sch.error());
      return false;
    }
    out = *std::move(sch);
    return true;
  }
};

template <>
struct parser_registry<schema> {
  using type = schema_parser;
};

namespace parsers {

constexpr auto schema = schema_parser{};

} // namespace parsers
} // namespace vast
