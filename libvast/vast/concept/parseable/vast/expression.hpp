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

struct predicate_operand_parser : parser_base<predicate_operand_parser> {
  using attribute = predicate::operand;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, predicate::operand& x) const;
};

template <>
struct parser_registry<predicate::operand> {
  using type = predicate_operand_parser;
};

namespace parsers {

static auto const predicate_operand = make_parser<vast::predicate::operand>();

} // namespace parsers

struct predicate_parser : parser_base<predicate_parser> {
  using attribute = predicate;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, predicate& a) const;
};

template <>
struct parser_registry<predicate> {
  using type = predicate_parser;
};

namespace parsers {

static auto const predicate = make_parser<vast::predicate>();

} // namespace parsers

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

static auto const expr = make_parser<vast::expression>();

} // namespace parsers
} // namespace vast
