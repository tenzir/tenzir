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

#ifndef VAST_CONCEPT_PARSEABLE_TO_HPP
#define VAST_CONCEPT_PARSEABLE_TO_HPP

#include <iterator>
#include <type_traits>

#include "vast/error.hpp"
#include "vast/expected.hpp"
#include "vast/concept/parseable/parse.hpp"

namespace vast {

template <class To, class Iterator>
auto to(Iterator& f, const Iterator& l)
  -> std::enable_if_t<is_parseable<Iterator, To>{}, expected<To>> {
  expected<To> t{To{}};
  if (!parse(f, l, *t))
    return make_error(ec::parse_error);
  return t;
}

template <class To, class Range>
auto to(Range&& rng)
  -> std::enable_if_t<
       is_parseable<decltype(std::begin(rng)), To>{}, expected<To>
     > {
  using std::begin;
  using std::end;
  auto f = begin(rng);
  auto l = end(rng);
  return to<To>(f, l);
}

template <class To, size_t N>
auto to(char const(&str)[N]) {
  auto first = str;
  auto last = str + N - 1; // No NUL byte.
  return to<To>(first, last);
}

} // namespace vast

#endif
