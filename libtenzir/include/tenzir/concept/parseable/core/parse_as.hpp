//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"

namespace tenzir {

/// Casts a parser's attribute to a specific type.
template <class Parser, class Attribute>
class as_parser : public parser_base<as_parser<Parser, Attribute>> {
public:
  using attribute = Attribute;

  constexpr as_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attr>
  bool parse(Iterator& f, const Iterator& l, Attr& a) const {
    attribute x;
    if (!parser_(f, l, x))
      return false;
    a = Attr(std::move(x));
    return true;
  }

private:
  Parser parser_;
};

template <class Attribute, parser Parser>
constexpr auto
parse_as(Parser&& p) -> as_parser<std::decay_t<Parser>, Attribute> {
  return as_parser<std::decay_t<Parser>, Attribute>{std::forward<Parser>(p)};
}

} // namespace tenzir
