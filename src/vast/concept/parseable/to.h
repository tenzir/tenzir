#ifndef VAST_CONCEPT_PARSEABLE_TO_H
#define VAST_CONCEPT_PARSEABLE_TO_H

#include <iterator>
#include <type_traits>

#include "vast/maybe.h"
#include "vast/concept/parseable/parse.h"

namespace vast {

template <typename To, typename Iterator>
auto to(Iterator& f, Iterator const& l)
  -> std::enable_if_t<is_parseable<Iterator, To>{}, maybe<To>> {
  maybe<To> t{To{}};
  if (!parse(f, l, *t))
    return nil;
  return t;
}

template <typename To, typename Range>
auto to(Range&& rng)
  -> std::enable_if_t<
       is_parseable<decltype(std::begin(rng)), To>{}, maybe<To>
     > {
  using std::begin;
  using std::end;
  auto f = begin(rng);
  auto l = end(rng);
  return to<To>(f, l);
}

template <typename To, size_t N>
auto to(char const(&str)[N]) {
  auto first = str;
  auto last = str + N - 1; // No NUL byte.
  return to<To>(first, last);
}

} // namespace vast

#endif
