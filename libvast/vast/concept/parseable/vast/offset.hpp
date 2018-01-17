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

#ifndef VAST_CONCEPT_PARSEABLE_VAST_OFFSET_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_OFFSET_HPP

#include "vast/offset.hpp"

#include "vast/concept/parseable/core/list.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"

namespace vast {

struct offset_parser : parser<offset_parser> {
  using attribute = offset;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
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

#endif
