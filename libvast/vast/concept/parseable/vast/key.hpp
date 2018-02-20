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

#ifndef VAST_CONCEPT_PARSEABLE_VAST_KEY_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_KEY_HPP

#include "vast/key.hpp"

#include "vast/concept/parseable/core/choice.hpp"
#include "vast/concept/parseable/core/list.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/core/plus.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {

struct key_parser : parser<key_parser> {
  using attribute = key;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    // FIXME: we currently cannot parse character sequences into containers,
    // e.g., (alpha | '_') >> +(alnum ...). Until we have enhanced the
    // framework, we'll just bail out when we find a colon at the beginning.
    if (f != l && *f == ':')
      return false;
    using namespace parsers;
    static auto p = +(alnum | chr{'_'} | chr{':'}) % '.';
    return p(f, l, a);
  }
};

template <>
struct parser_registry<key> {
  using type = key_parser;
};

namespace parsers {

static auto const key = make_parser<vast::key>();

} // namespace parsers

} // namespace vast

#endif
