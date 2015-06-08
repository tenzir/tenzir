#ifndef VAST_CONCEPT_PARSEABLE_CORE_LITERAL_H
#define VAST_CONCEPT_PARSEABLE_CORE_LITERAL_H

#include "vast/concept/parseable/detail/as_parser.h"

namespace vast {

template <typename T>
auto literal(T x)
  -> std::enable_if_t<
       detail::is_convertible_to_unary_parser<T>{},
       decltype(detail::as_parser(x))
     >
{
  return detail::as_parser(x);
}

namespace parsers {

template <typename T>
auto lit(T x)
{
  return literal(x);
}

} // namespace parsers
} // namespace vast

#endif
