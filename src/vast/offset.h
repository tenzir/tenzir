#ifndef VAST_OFFSET_H
#define VAST_OFFSET_H

#include <vector>
#include "vast/print.h"
#include "vast/util/parse.h"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : std::vector<size_t>,
                util::parsable<offset>
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
    return util::print_delimited(',', o.begin(), o.end(), out);
  }

  template <typename Iterator>
  bool parse(Iterator& begin, Iterator end)
  {
    size_t i;
    while (begin != end)
    {
      if (! util::parse_positive_decimal(begin, end, i))
        return false;
      push_back(i);
      if (begin != end && *begin++ != ',')
        return false;
    }
    return true;
  }
};

} // namespace vast

#endif
