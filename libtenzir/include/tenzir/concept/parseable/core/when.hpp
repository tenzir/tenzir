//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2017 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"

namespace tenzir {

template <class Parser, class Condition>
class when_parser : public parser_base<when_parser<Parser, Condition>> {
public:
  using attribute = typename Parser::attribute;

  when_parser(Parser p, Condition fun) : parser_{std::move(p)}, condition_(fun) {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    return condition_() and parser_(f, l, x);
  }

private:
  Parser parser_;
  Condition condition_;
};

} // namespace tenzir
