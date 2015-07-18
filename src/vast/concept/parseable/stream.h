#ifndef VAST_CONCEPT_PARSEABLE_STREAM_H
#define VAST_CONCEPT_PARSEABLE_STREAM_H

#include <istream>
#include <type_traits>

#include "vast/concept/parseable/parse.h"

namespace vast {

template <typename CharT, typename Traits, typename T>
auto operator>>(std::basic_istream<CharT, Traits>& in, T& x)
  -> std::enable_if_t<
       is_parseable<std::istreambuf_iterator<CharT>, T>::value, decltype(in)
     >
{
  using vast::parse; // enable ADL
  std::istreambuf_iterator<CharT> begin{in}, end;
  if (! parse(begin, end, x))
    in.setstate(std::ios_base::failbit);
  return in;
}

} // namespace vast

#endif
