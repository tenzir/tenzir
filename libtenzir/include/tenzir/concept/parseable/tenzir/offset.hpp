//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/list.hpp"
#include "tenzir/concept/parseable/core/operators.hpp"
#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/offset.hpp"

namespace tenzir {

struct offset_parser : parser_base<offset_parser> {
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

static auto const offset = make_parser<tenzir::offset>();

} // namespace parsers

} // namespace tenzir
