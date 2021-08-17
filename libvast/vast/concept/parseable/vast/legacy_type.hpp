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
#include "vast/concept/parseable/string/any.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/string/literal.hpp"
#include "vast/legacy_type.hpp"

namespace vast {

/// Parses a type into an intermediate representation.
/// References to user defined types are mapped to `legacy_none_type` and
/// need to be resolved later.
struct type_parser : parser_base<type_parser> {
  using attribute = type;

  // Comments until the end of line.
  static constexpr auto comment
    = ignore(parsers::lit{"//"} >> *(parsers::any - '\n'));

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
