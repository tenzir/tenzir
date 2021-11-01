//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"
#include "vast/concept/parseable/vast/legacy_type.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"

namespace vast {

using symbol_map = std::unordered_map<std::string, legacy_type>;

/// Converts a symbol_map into a schema. Can use an additional symbol table
/// as context.
struct symbol_resolver {
  caf::expected<legacy_type> lookup(const std::string& key) {
    // First we check if the key is already locally resolved.
    auto resolved_symbol = resolved.find(key);
    if (resolved_symbol != resolved.end())
      return resolved_symbol->second;
    // Then we check if it is an unresolved local type.
    auto next = local.find(key);
    if (next != local.end())
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
  caf::expected<legacy_type> operator()(Type x) {
    return std::move(x);
  }

  caf::expected<legacy_type> operator()(const legacy_none_type& x) {
    VAST_ASSERT(!x.name().empty());
    auto concrete = lookup(x.name());
    if (!concrete)
      return concrete.error();
    return concrete->update_attributes(x.attributes());
  }

  caf::expected<legacy_type> operator()(legacy_alias_type x) {
    auto y = caf::visit(*this, x.value_type);
    if (!y)
      return y.error();
    x.value_type = *y;
    return std::move(x);
  }

  caf::expected<legacy_type> operator()(legacy_list_type x) {
    auto y = caf::visit(*this, x.value_type);
    if (!y)
      return y.error();
    x.value_type = *y;
    if (caf::holds_alternative<legacy_record_type>(x.value_type)
        && !has_skip_attribute(x))
      x.update_attributes({{"skip", caf::none}});
    return std::move(x);
  }

  caf::expected<legacy_type> operator()(legacy_map_type x) {
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

  caf::expected<legacy_type> operator()(legacy_record_type x) {
    for (auto& [field_name, field_type] : x.fields) {
      auto y = caf::visit(*this, field_type);
      if (!y)
        return y.error();
      field_type = *y;
    }
    if (has_attribute(x, "$algebra")) {
      VAST_ASSERT(x.fields.size() >= 2);
      const auto* base = caf::get_if<legacy_record_type>(&x.fields[0].type);
      VAST_ASSERT(base);
      auto acc = *base;
      auto it = ++x.fields.begin();
      for (; it < x.fields.end(); ++it) {
        const auto* rhs = caf::get_if<legacy_record_type>(&it->type);
        VAST_ASSERT(rhs);
        if (it->name == "+") {
          auto result = merge(acc, *rhs);
          if (!result)
            return result.error();
          acc = *result;
        } else if (it->name == "<+") {
          acc = priority_merge(acc, *rhs, merge_policy::prefer_left);
        } else if (it->name == "+>") {
          acc = priority_merge(acc, *rhs, merge_policy::prefer_right);
        } else if (it->name == "-") {
          std::vector<std::string_view> path;
          for (const auto& f : rhs->fields)
            path.emplace_back(f.name);
          auto acc_removed = remove_field(acc, path);
          if (!acc_removed)
            return caf::make_error( //
              ec::parse_error,
              fmt::format("cannot delete non-existing field {} from type {}",
                          fmt::join(path, "."), to_string(acc)));
          acc = *std::move(acc_removed);
        } else
          // Invalid operation.
          VAST_ASSERT(true);
      }
      // TODO: Consider lifiting the following restriction.
      if (acc.fields.empty())
        return caf::make_error(
          ec::parse_error, fmt::format("type modifications produced an empty "
                                       "record named {}; this is not "
                                       "supported.",
                                       x.name()));
      for (const auto& field : acc.fields)
        VAST_ASSERT(!field.name.empty());
      return acc.name(x.name());
    }
    return std::move(x);
  }

  caf::expected<legacy_type> resolve(symbol_map::iterator next) {
    auto value = std::move(*next);
    if (resolved.find(value.first) != resolved.end())
      return caf::make_error(ec::parse_error, "duplicate definition of",
                             value.first);
    local.erase(next);
    auto x = caf::visit(*this, value.second);
    if (!x)
      return x.error();
    auto [iter, inserted] = resolved.emplace(value.first, std::move(*x));
    if (!inserted)
      return caf::make_error(ec::parse_error, "failed to extend resolved "
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
  // can either be part of the same local set or provided in the global table.
  // If the symbol is from the local working set but hasn't been resolved
  // itself, the resolution of the current type is suspended and the required
  // symbol is prioritized.
  // That means that a single iteration of this loop can remove between 1 and
  // all remaining elements from the local set.
  caf::expected<schema> resolve() {
    while (!local.empty())
      if (auto x = resolve(local.begin()); !x)
        return x.error();
    // Finally we replace the now empty local set with the set of resolved
    // symbols for further use by the caller.
    local = std::move(resolved);
    return sch;
  }

  const symbol_map& global;
  // This is an in-out parameter so the use site of the symbol_resolver can
  // use the resolved symbol_map to resolve symbols that are parsed later.
  symbol_map& local;
  symbol_map resolved = {};
  schema sch = {};
};

struct symbol_map_parser : parser_base<symbol_map_parser> {
  using attribute = symbol_map;

  static constexpr auto skp = type_parser::skp;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& out) const {
    static_assert(detail::is_any_v<Attribute, attribute, unused_type>);
    bool duplicate_symbol = false;
    auto to_type = [&](std::tuple<std::string, legacy_type> t) -> legacy_type {
      auto [name, ty] = std::move(t);
      // If the type has already a name, we're dealing with a symbol and have
      // to create an alias.
      if (!ty.name().empty())
        ty = legacy_alias_type{ty}; // TODO: attributes
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

struct schema_parser : parser_base<schema_parser> {
  using attribute = schema;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& out) const {
    symbol_map global;
    symbol_map local;
    auto p = symbol_map_parser{};
    if (!p(f, l, local))
      return false;
    auto r = symbol_resolver{global, local};
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
