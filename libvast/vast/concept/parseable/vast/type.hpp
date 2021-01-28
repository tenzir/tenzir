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
#include "vast/concept/parseable/string/any.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/string/literal.hpp"
#include "vast/concept/parseable/string/symbol_table.hpp"
#include "vast/type.hpp"

namespace vast {

/// A symbol table parser for types.
class type_table : public parser<type_table> {
public:
  using attribute = type;

  /// Constructs an empty type table.
  type_table() = default;

  /// Construct a type table from a list of name-value pairs.
  /// @warning This constructor simply calls `add()` without checking the
  /// return value, so the caller must ensure that the list of names is unique
  /// because only the first pair gets picked.
  explicit type_table(std::initializer_list<std::pair<std::string, type>> init) {
    for (auto& pair : init)
      add(pair.first, pair.second);
  }

  /// Adds a type to the type table.
  /// @param name The name of the type.
  /// @param t The type to bind to *name*.
  /// @returns `true` iff the type registration succeeded.
  bool add(const std::string& name, type t) {
    if (name.empty() || name != t.name())
      return false;
    t.name(name);
    return symbols_.symbols.emplace(name, t).second;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    return symbols_(f, l, a);
  }

private:
  symbol_table<type> symbols_;
};

/// Parses a type into an intermediate representation.
/// References to user defined types are mapped to `none_type` and
/// need to be resolved later.
struct type_parser : parser<type_parser> {
  using attribute = type;

  // Comments until the end of line.
  // clang-format off
  static constexpr auto comment
    = ignore(parsers::lit{"//"} >> *(parsers::any - '\n'));

  /// The parser for an identifier.
  static constexpr auto id
    = +( parsers::alnum
       | parsers::ch<'_'>
       | parsers::ch<'.'>
       );
  // clang-format on

  // Skips all irrelevant tokens.
  static constexpr auto skp = ignore(*(parsers::space | comment));

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const;
};

template <>
struct parser_registry<type> {
  using type = type_parser;
};

namespace parsers {

inline const auto type = type_parser{};

} // namespace parsers
} // namespace vast
