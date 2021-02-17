/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/parseable/numeric/bool.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/json.hpp"

namespace vast {

namespace parsers {

// clang-format off
static auto const json_boolean
  = parsers::boolean;
static auto const json_int
  = parsers::i64;
static auto const json_count
  = (parsers::hex_prefix >> parsers::hex64)
  | parsers::u64;
static auto const json_number
  = (parsers::hex_prefix >> parsers::hex64 ->* [](uint64_t x) {
        return detail::narrow_cast<json::number>(x); })
  | parsers::real_opt_dot;
// clang-format on

} // namespace parsers

struct json_parser : parser<json_parser> {
  using attribute = json;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, json& x) const {
    using namespace parsers;
    using namespace parser_literals;
    rule<Iterator, json> j;
    auto ws = ignore(*parsers::space);
    auto lbracket = ws >> '[' >> ws;
    auto rbracket = ws >> ']' >> ws;
    auto lbrace = ws >> '{' >> ws;
    auto rbrace = ws >> '}' >> ws;
    auto delim = ws >> ',' >> ws;
    // clang-format off
    auto null = "null"_p ->* [] { return json::null{}; };
    // clang-format on
    auto string = parsers::qqstr;
    auto array = as<json::array>(lbracket >> ~(ref(j) % delim) >> rbracket);
    auto key_value = string >> ws >> ':' >> ws >> ref(j);
    auto object = as<json::object>(lbrace >> ~(key_value % delim) >> rbrace);
    // clang-format off
    j = ws >> ( null
              | json_boolean
              | json_number
              | string
              | array
              | object
              );
    // clang-format on
    return j(f, l, x);
  }
};

template <>
struct parser_registry<json> {
  using type = json_parser;
};

namespace parsers {

static auto const json = make_parser<vast::json>();

} // namespace parsers

} // namespace vast
