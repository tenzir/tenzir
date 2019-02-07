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

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// Matches the input when the input is exhausted.
class end_of_input_parser : public parser<end_of_input_parser> {
public:
  using attribute = unused_type;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute&) const {
    return f == l;
  }
};

namespace parsers {

auto const eoi = end_of_input_parser{};

} // namespace parsers
} // namespace vast
