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

using symbol_map = std::unordered_map<std::string, type>;

/// Converts a symbol_map into a schema. Can use an additional symbol table
/// as context.
struct symbol_resolver {
  caf::expected<type> lookup(const std::string& key) {
    // First we check if the key is already locally resolved.
    auto local_symbol = local.find(key);
    if (local_symbol != local.end())
      return local_symbol->second;
    // Then we check if it is an unresolved local type.
    auto next = working.find(key);
    if (next != working.end())
      return resolve(next);
    // Finally, we look into the global types, This is in last place because
    // they have lower precedence, i.e. local definitions are allowed to
    // shadow global ones.
    auto global_symbol = global.find(key);
    if (global_symbol != global.end())
      return global_symbol->second;
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

  caf::expected<type> resolve(symbol_map::iterator next) {
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

  // Main entry point. The algorithm starts by popping of an entry from the set
  // of parsed symbols. It walks over its definition and checks all
  // "placeholder" symbols (all those that are not builtin types). Once a
  // placeholder is found it is going to be replaced by its defintion, which
  // can either be part of the same working set or provided in the global table.
  // If the symbol is from the local working set but hasn't been resolved
  // itself, the resolution of the current type is suspended and the required
  // symbol is prioritized.
  // That means that a single iteration of this loop can remove between 1 and
  // all remaining elements from the working set.
  caf::expected<schema> resolve() {
    while (!working.empty())
      if (auto x = resolve(working.begin()); !x)
        return x.error();
    return sch;
  }

  const symbol_map& global;
  symbol_map working;
  symbol_map local = {};
  schema sch = {};
};

struct symbol_map_parser : parser<symbol_map_parser> {
  using attribute = symbol_map;

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
      if (!out.emplace(name, ty).second) {
        VAST_ERROR("multiple definitions of {} detected", name);
        duplicate_symbol = true;
      }
      return ty;
    };
    // We can't use & because the operand is a parser, and our DSL overloads &.
    auto tp = parsers::type;
    // clang-format off
    auto decl
      = ("type" >> skp >> parsers::identifier >> skp >> '=' >> skp >> tp)
          ->* to_type;
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
struct parser_registry<symbol_map> {
  using type = symbol_map_parser;
};

namespace parsers {

constexpr auto symbol_map = symbol_map_parser{};

} // namespace parsers

struct schema_parser : parser<schema_parser> {
  using attribute = schema;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& out) const {
    symbol_map global;
    symbol_map working;
    auto p = symbol_map_parser{};
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
