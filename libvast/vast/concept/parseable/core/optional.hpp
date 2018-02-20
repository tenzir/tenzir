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

#ifndef VAST_CONCEPT_PARSEABLE_CORE_OPTIONAL_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_OPTIONAL_HPP

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"
#include "vast/optional.hpp"

namespace vast {

template <class Parser>
class optional_parser : public parser<optional_parser<Parser>> {
public:
  using inner_attribute =
    typename detail::attr_fold<typename Parser::attribute>::type;

  using attribute =
    std::conditional_t<
      std::is_same<inner_attribute, unused_type>{},
      unused_type,
      optional<inner_attribute>
    >;

  explicit optional_parser(Parser p)
    : parser_{std::move(p)} {
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    parser_(f, l, unused);
    return true;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    inner_attribute attr;
    if (parser_(f, l, attr))
      a = std::move(attr);
    return true;
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
