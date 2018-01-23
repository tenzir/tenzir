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

#ifndef VAST_CONCEPT_PARSEABLE_CORE_IGNORE_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_IGNORE_HPP

#include <type_traits>

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// Wraps a parser and ignores its attribute.
template <typename Parser>
class ignore_parser : public parser<ignore_parser<Parser>> {
public:
  using attribute = unused_type;

  explicit ignore_parser(Parser p) : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute&) const {
    return parser_(f, l, unused);
  }

private:
  Parser parser_;
};

template <typename Parser>
auto ignore(Parser&& p)
-> std::enable_if_t<
     is_parser<std::decay_t<Parser>>::value,
     ignore_parser<std::decay_t<Parser>>
   > {
  return ignore_parser<std::decay_t<Parser>>{std::move(p)};
}

} // namespace vast

#endif
