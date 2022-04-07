//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/string/string.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"
#include "vast/concept/parseable/vast/integer.hpp"
#include "vast/concept/parseable/vast/pattern.hpp"
#include "vast/concept/parseable/vast/si.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/data.hpp"

#include <caf/none.hpp>

namespace vast {

struct pipe_parser : parser_base<pipe_parser> {
  using attribute = data;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    static auto p = make<Iterator>();
    return p(f, l, a);
  }

private:
  template <class Iterator>
  static auto make() {
    using namespace parser_literals;
    rule<Iterator, data> p;
    auto ws = ignore(*parsers::space);
    auto x = ws >> ref(p) >> ws;
    auto kvp = x >> "->" >> x;
    auto trailing_comma = ~(',' >> ws);
    auto named_field = ws >> parsers::identifier >> ":" >> x;
    auto record_parser
      = '<' >> ~as<record>(named_field % ',') >> trailing_comma >> '>';
    p = parsers::time | parsers::duration | parsers::net | parsers::addr
        | parsers::real | parsers::count | parsers::integer | parsers::tf
        | parsers::qqstr | parsers::pattern
        | '[' >> ~(x % ',') >> trailing_comma >> ']'
        | '{' >> ~as<map>(kvp % ',') >> trailing_comma >> '}' | record_parser
        | as<caf::none_t>("nil"_p) | as<caf::none_t>(parsers::ch<'_'>);
    // clang-format on
    auto parameter_char = (parsers::alnum | parsers::ch<'-'>);
    auto parameter = ws >> +parameter_char >> ":" >> x;
    auto op_parser
      = ws >> (parsers::identifier >> ws >> ~as<record>(
                 '(' >> (parameter % ',') >> trailing_comma >> ')')
               >> ws)
                  ->*[p](std::tuple<std::string, record> t) -> data {
      return record{
        {std::get<0>(t), std::get<1>(t)},
      };
    };
    auto pipe_parser = ~as<list>(op_parser % '|');

    return pipe_parser;
  }
};

namespace parsers {

static auto const pipe = pipe_parser{};

} // namespace parsers
} // namespace vast
