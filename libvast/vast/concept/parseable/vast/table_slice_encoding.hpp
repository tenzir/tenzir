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

#include "vast/fwd.hpp"

#include "vast/concept/parseable/core/literal.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string/char.hpp"

namespace vast {

struct table_slice_encoding_parser : parser<table_slice_encoding_parser> {
  using attribute = table_slice_encoding;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    using namespace parser_literals;
    // clang-format off
    auto p = "arrow"_p ->* [] { return table_slice_encoding::arrow; }
           | "msgpack"_p ->* [] { return table_slice_encoding::msgpack; };
    // clang-format on
    return p(f, l, a);
  }
};

template <>
struct parser_registry<table_slice_encoding> {
  using type = table_slice_encoding_parser;
};

namespace parsers {

static auto const table_slice_encoding = table_slice_encoding_parser{};

} // namespace parsers
} // namespace vast
