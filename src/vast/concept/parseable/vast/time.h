#ifndef VAST_CONCEPT_PARSEABLE_VAST_TIME_H
#define VAST_CONCEPT_PARSEABLE_VAST_TIME_H

#include "vast/access.h"
#include "vast/time.h"
#include "vast/concept/parseable/core/optional.h"
#include "vast/concept/parseable/core/sequence.h"
#include "vast/concept/parseable/numeric/real.h"

namespace vast {

struct time_duration_parser : parser<time_duration_parser>
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

namespace detail {

struct ymd_parser : vast::parser<ymd_parser>
{
  using attribute = std::tm;

  // YYYY(-MM(-DD))
  static auto make()
  {
    auto delim = ignore(char_parser{'-'});
    auto year = integral_parser<unsigned, 4, 4>{};
    auto mon = integral_parser<unsigned, 2, 2>{};
    auto day = integral_parser<unsigned, 2, 2>{};
    return year >> ~(delim >> mon >> ~(delim >> day));
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    using std::get;
    static auto p = make();
    auto ymd = decltype(p)::attribute{};
    if (p.parse(f, l, ymd))
    {
      a.tm_year = get<0>(ymd) - 1900;
      if (get<1>(ymd))
      {
        a.tm_mon = get<0>(*get<1>(ymd)) - 1;
        if (get<1>(*get<1>(ymd)))
          a.tm_mday = *get<1>(*get<1>(ymd));
      }
      return true;
    }
    return false;
  }
};

} // detail

struct time_point_parser : parser<time_point_parser>
{
  using attribute = time::point;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    return parser_.parse(f, l, unused);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    std::tm tm;
    if (! parser_.parse(f, l, tm))
      return false;
    a = time::point::from_tm(tm);
    return true;
  }

  detail::ymd_parser parser_;
};

template <>
struct parser_registry<time::point>
{
  using type = time_point_parser;
};

} // namespace vast

#endif
