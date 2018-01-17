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

#ifndef VAST_CONCEPT_PARSEABLE_CORE_MAYBE_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_MAYBE_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// Like ::optional_parser, but exposes `T` instead of `optional<T>` as
/// attribute.
template <typename Parser>
class maybe_parser : public parser<maybe_parser<Parser>> {
public:
  using attribute = typename Parser::attribute;

  explicit maybe_parser(Parser p)
    : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    parser_(f, l, a);
    return true;
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
