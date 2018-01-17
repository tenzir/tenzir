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

#ifndef VAST_CONCEPT_PARSEABLE_CORE_NOT_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_NOT_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <typename Parser>
class not_parser : public parser<not_parser<Parser>> {
public:
  using attribute = unused_type;

  explicit not_parser(Parser p) : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute&) const {
    auto i = f; // Do not consume input.
    return !parser_(i, l, unused);
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
