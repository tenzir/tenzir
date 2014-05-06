#ifndef VAST_OFFSET_H
#define VAST_OFFSET_H

#include <vector>
#include "vast/print.h"
#include "vast/parse.h"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : std::vector<size_t>
{
  using super = std::vector<size_t>;

  offset() = default;

  offset(super::const_iterator begin, super::const_iterator end)
    : super{begin, end}
  {
  }

  offset(super v)
    : super{std::move(v)}
  {
  }

  offset(std::initializer_list<size_t> list)
    : super{std::move(list)}
  {
  }

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
