//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/detail/container.hpp"

#include <vector>

namespace tenzir {

template <class Parser>
class kleene_parser : public parser_base<kleene_parser<Parser>> {
public:
  using container = detail::container_t<typename Parser::attribute>;
  using attribute = typename container::attribute;

  constexpr explicit kleene_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    while (container::parse(parser_, f, l, a))
      ;
    return true;
  }

private:
  Parser parser_;
};

} // namespace tenzir
