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

/// A parser that ingores the next *n* bytes.
class skip_parser : public parser<skip_parser> {
public:
  using attribute = unused_type;

  explicit skip_parser(const size_t& n) : n_{n} {
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    if (f + n_ >= l)
      return false;
    f += n_;
    return true;
  }

private:
  const size_t& n_;
};

namespace parsers {

inline auto skip(const size_t& n) {
  return skip_parser{n};
}

} // namespace parsers
} // namespace vast

