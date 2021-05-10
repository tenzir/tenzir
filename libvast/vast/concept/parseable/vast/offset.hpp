//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/list.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/offset.hpp"

namespace vast {

struct offset_parser : parser<offset_parser> {
  using attribute = offset;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    static auto p = parsers::u32 % ',';
    return p(f, l, a);
  }
};

template <>
struct parser_registry<offset> {
  using type = offset_parser;
};

namespace parsers {

static auto const offset = make_parser<vast::offset>();

} // namespace parsers

} // namespace vast
