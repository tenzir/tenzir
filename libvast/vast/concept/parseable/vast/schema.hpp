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

struct shared_schema_parser : parser<shared_schema_parser> {
  using symbol_buffer = std::unordered_map<std::string, type>;
  using attribute = schema;

  shared_schema_parser(const symbol_buffer& gs, symbol_buffer& ls)
    : global_symbols{gs}, local_symbols{ls} {
    // nop
  }

  static constexpr auto id = type_parser::id;
  static constexpr auto skp = type_parser::skp;

  struct resolver {
    caf::expected<type> lookup(const std::string& key) {
      // First we check if the key is already locally resolved.
      auto local = parent.local_symbols.find(key);
      if (local != parent.local_symbols.end())
        return local->second;
      // Then we check if it is an unresolved local type.
      auto next = sb.find(key);
      if (next != sb.end())
        return resolve(next);
      // Finally, we look into the global types, This is in last place because
      // they have lower precedence, i.e. local definitions are allowed to
      // shadow global ones.
      auto global = parent.global_symbols.find(key);
      if (global != parent.global_symbols.end())
        return global->second;
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

    caf::expected<type> resolve(symbol_buffer::iterator next) {
      auto value = std::move(*next);
      if (parent.local_symbols.find(value.first) != parent.local_symbols.end())
        return caf::make_error(ec::parse_error, "duplicate definition of",
                               value.first);
      sb.erase(next);
      auto x = caf::visit(*this, value.second);
      if (!x)
        return x.error();
      auto [iter, inserted]
        = parent.local_symbols.emplace(value.first, std::move(*x));
      if (!inserted)
        return caf::make_error(ec::parse_error, "failed to extend local "
                                                "symbols");
      auto added = sch.add(iter->second);
      if (!added)
        return caf::make_error(ec::parse_error, "failed to insert type",
                               value.first);
      return iter->second;
    }

    caf::expected<schema> run() {
      while (!sb.empty()) {
        if (auto x = resolve(sb.begin()); !x)
          return x.error();
      }
      return sch;
    }

    const shared_schema_parser& parent;
    symbol_buffer sb;
    schema sch = {};
  };

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& sch) const {
    static_assert(detail::is_any_v<Attribute, attribute, unused_type>);
    symbol_buffer sb;
    bool table_failed = false;
    auto to_type = [&](std::tuple<std::string, type> t) -> type {
      auto [name, ty] = std::move(t);
      // If the type has already a name, we're dealing with a symbol and have
      // to create an alias.
      if (!ty.name().empty())
        ty = alias_type{ty}; // TODO: attributes
      ty.name(name);
      auto emplace_result = sb.emplace(name, ty);
      if (!emplace_result.second) {
        VAST_ERROR("multiple definitions of {} detected", name);
        table_failed = true;
      }
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
    if (table_failed)
      return false;
    auto r = resolver{*this, std::move(sb)};
    auto res = r.run();
    if (!res) {
      VAST_ERROR("schema parser failed: {}", render(res.error()));
      return false;
    }
    sch = *std::move(res);
    return true;
  }

  const symbol_buffer& global_symbols;
  symbol_buffer& local_symbols;
};

struct schema_parser : parser<schema_parser> {
  using attribute = schema;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& sch) const {
    shared_schema_parser::symbol_buffer gs;
    shared_schema_parser::symbol_buffer ls;
    auto p = shared_schema_parser{gs, ls};
    return p(f, l, sch);
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
