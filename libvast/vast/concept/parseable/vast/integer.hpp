//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/parse.hpp"
#include "vast/concept/parseable/vast/si.hpp"
#include "vast/data/integer.hpp"

namespace vast {

struct integer_parser : parser<integer_parser> {
  using attribute = integer;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    return si_parser<integer::value_type>{}(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, integer& x) const {
    return si_parser<integer::value_type>{}(f, l, x.value);
  }
};

template <>
struct parser_registry<integer> {
  using type = integer_parser;
};

namespace parsers {
auto const integer = integer_parser{};
} // namespace parsers

} // namespace vast
