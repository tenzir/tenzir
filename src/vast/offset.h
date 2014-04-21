#ifndef VAST_OFFSET_H
#define VAST_OFFSET_H

#include <vector>
#include "vast/util/print.h"
#include "vast/util/parse.h"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : std::vector<size_t>,
                util::printable<offset>,
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
  bool print(Iterator& out) const
  {
    auto f = begin();
    auto l = end();
    while (f != l)
      // FIXME: there's a bug in the printing concept: if we don't cast *f to
      // an uint64_t, it complains about an ambiguous call to print(...). 
      if (render(out, uint64_t(*f)) && ++f != l && ! render(out, ","))
        return false;
    return true;
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
