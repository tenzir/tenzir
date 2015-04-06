#ifndef VAST_CONCEPT_PARSEABLE_TO_H
#define VAST_CONCEPT_PARSEABLE_TO_H

#include <type_traits>

#include "vast/trial.h"
#include "vast/concept/parseable/core/parse.h"

namespace vast {

template <typename To, typename Iterator>
auto to(Iterator& f, Iterator const& l)
  -> std::enable_if_t<is_parseable<Iterator, To>::value, trial<To>>
{
  trial<To> t{To{}};
  if (! parse(f, l, *t))
    t = error{"parsing failed"};
  return t;
}

template <typename To, typename Range>
auto to(Range&& rng)
  -> std::enable_if_t<
       is_parseable<decltype(std::begin(rng)), To>::value, trial<To>
     >
{
  using std::begin;
  using std::endl;
  auto f = begin(rng);
  auto l = end(rng);
  return to<To>(f, l);
}

template <typename To, size_t N>
auto to(char const (&str)[N])
{
  auto first = str;
  auto last = str + N - 1; // No NUL byte.
  return to<To>(first, last);
}

} // namespace vast

#endif
