//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/expression.hpp"

namespace vast {

struct operand_parser : parser_base<operand_parser> {
  using attribute = operand;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, attribute& a) const;
};

template <>
struct parser_registry<operand> {
  using type = operand_parser;
};

struct predicate_parser : parser_base<predicate_parser> {
  using attribute = predicate;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, attribute& a) const;
};

template <>
struct parser_registry<predicate> {
  using type = predicate_parser;
};

struct expression_parser : parser_base<expression_parser> {
  using attribute = expression;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const;
};

template <>
struct parser_registry<expression> {
  using type = expression_parser;
};

namespace parsers {

static auto const predicate = make_parser<vast::predicate>();
static auto const operand = make_parser<vast::operand>();
static auto const expr = make_parser<vast::expression>();

} // namespace parsers
} // namespace vast
