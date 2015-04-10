#ifndef VAST_CONCEPT_PARSEABLE_VAST_TIME_H
#define VAST_CONCEPT_PARSEABLE_VAST_TIME_H

#include "vast/access.h"
#include "vast/time.h"
#include "vast/concept/parseable/numeric/real.h"

namespace vast {

struct time_duration_parser : vast::parser<time_duration_parser>
{
  using attribute = time::duration;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    // First try to parse the number as fractional timestamp.
    static auto pr = real_parser<double>{};
    double d;
    if (pr.parse(f, l, d))
    {
      // We currently don't support units in fractional timestamps and
      // therefore assume seconds when we get a fractional timestamp.
      a = time::double_seconds{d};
      return true;
    }
    // If we don't have a fractional value, try a plain integral value with
    // units.
    static auto pi = integral_parser<int64_t>{};
    int64_t i;
    if (! pi.parse(f, l, i))
      return false;
    // No suffix implies seconds.
    if (f == l)
    {
      a = time::seconds(i);
      return true;
    }
    // Parse the suffix.
    auto save = f;
    switch (*f++)
    {
      case 'n':
        if (f != l && *f++ == 's')
        {
          a = time::nanoseconds(i);
          return true;
        }
        break;
      case 'u':
        if (f != l && *f++ == 's')
        {
          a = time::microseconds(i);
          return true;
        }
        break;
      case 'm':
        if (f != l && *f++ == 's')
          a = time::milliseconds(i);
        else
          a = time::minutes(i);
        return true;
      case 's':
        a = time::seconds(i);
        return true;
      case 'h':
        a = time::hours(i);
        return true;
    }
    f = save;
    return false;
  }
};

template <>
struct parser_registry<time::duration>
{
  using type = time_duration_parser;
};

} // namespace vast

#endif
