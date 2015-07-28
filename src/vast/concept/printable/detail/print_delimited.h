#ifndef VAST_CONCEPT_PRINTABLE_DETAIL_PRINT_DELIMITED_H
#define VAST_CONCEPT_PRINTABLE_DETAIL_PRINT_DELIMITED_H

#include "vast/concept/printable/print.h"
#include "vast/concept/support/unused_type.h"

namespace vast {
namespace detail {

/// Prints a delimited Iterator range.
template <
  typename T,
  typename Delimiter,
  typename InputIterator,
  typename OutputIterator
>
bool print_delimited(InputIterator begin, InputIterator end,
                     OutputIterator&& out)
{
  static auto const printer = make_printer<T>{};
  static auto const delim = Delimiter{};
  if (begin == end)
    return true;
  if (! printer.print(out, *begin))
    return false;
  while (++begin != end)
    if (! (delim.print(out, unused) && printer.print(out, *begin)))
      return false;
  return true;
}

/// Prints a delimited Iterator range.
template <typename InputIterator, typename OutputIterator, typename Delimiter>
bool print_delimited(InputIterator begin, InputIterator end,
                     OutputIterator&& out, Delimiter const& delim)
{
  if (begin == end)
    return true;
  if (! print(out, *begin))
    return false;
  while (++begin != end)
    if (! (print(out, delim) && print(out, *begin)))
      return false;
  return true;
}

} // namespace detail
} // namespace vast

#endif
