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

#include <vector>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/container.hpp"

namespace vast {

template <class Parser>
class kleene_parser : public parser<kleene_parser<Parser>> {
public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  explicit kleene_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    while (container::parse(parser_, f, l, a))
      ;
    return true;
  }

private:
  Parser parser_;
};

} // namespace vast

