//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <class Parser, class Condition>
class when_parser : public parser_base<when_parser<Parser, Condition>> {
public:
  using attribute = typename Parser::attribute;

  when_parser(Parser p, Condition fun) : parser_{std::move(p)}, condition_(fun) {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    return condition_() && parser_(f, l, x);
  }

private:
  Parser parser_;
  Condition condition_;
};

} // namespace vast


