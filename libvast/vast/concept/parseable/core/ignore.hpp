// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <type_traits>

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// Wraps a parser and ignores its attribute.
template <class Parser>
class ignore_parser : public parser<ignore_parser<Parser>> {
public:
  using attribute = unused_type;

  constexpr explicit ignore_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute&) const {
    return parser_(f, l, unused);
  }

private:
  Parser parser_;
};

template <class Parser>
constexpr auto ignore(Parser&& p)
  -> std::enable_if_t<is_parser_v<std::decay_t<Parser>>,
                      ignore_parser<std::decay_t<Parser>>> {
  return ignore_parser<std::decay_t<Parser>>{std::move(p)};
}

} // namespace vast

