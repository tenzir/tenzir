#ifndef VAST_OFFSET_H
#define VAST_OFFSET_H

#include "vast/print.h"
#include "vast/parse.h"
#include "vast/util/stack_vector.h"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : util::stack_vector<size_t, 4>
{
  using util::stack_vector<size_t, 4>::stack_vector;

  template <typename Iterator>
  friend trial<void> print(offset const& o, Iterator&& out)
  {
    return print_delimited(',', o.begin(), o.end(), out);
  }

  template <typename Iterator>
  friend trial<void> parse(offset& o, Iterator& begin, Iterator end)
  {
    while (begin != end)
    {
      size_t i;
      auto t = parse_positive_decimal(i, begin, end);
      if (! t)
        return error{"expected digit"} + t.error();

      o.push_back(i);

      if (begin != end && *begin++ != ',')
        return error{"expected comma"};
    }

    return nothing;
  }
};

} // namespace vast

#endif
