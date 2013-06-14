#ifndef VAST_DETAIL_PARSER_TIME_POINT_H
#define VAST_DETAIL_PARSER_TIME_POINT_H

#include "vast/detail/parser/duration.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct time_point : qi::grammar<Iterator, vast::time_point(), skipper<Iterator>>
{
  struct absolute_time
  {
    template <typename>
    struct result
    {
      typedef vast::time_point type;
    };

    vast::time_point operator()(time_range r) const
    {
      return r;
    }
  };

  struct initializer
  {
    initializer(vast::time_point& p)
      : p(p)
    {
    }

    template <typename>
    struct result
    {
      typedef void type;
    };

    void operator()(int) const
    {
      p = now();
    }

    vast::time_point& p;
  };

  struct adder
  {
    adder(vast::time_point& p)
      : p(p)
    {
    }

    template <typename, typename, typename>
    struct result
    {
      typedef void type;
    };

    void operator()(int tag, int64_t n, bool negate) const
    {
      switch (tag)
      {
        default:
          assert(! "invalid tag");
          break;
        case 0:
          p += time_range(std::chrono::nanoseconds(negate ? -n : n));
          break;
        case 1:
          p += time_range(std::chrono::microseconds(negate ? -n : n));
          break;
        case 2:
          p += time_range(std::chrono::milliseconds(negate ? -n : n));
          break;
        case 3:
          p += time_range(std::chrono::seconds(negate ? -n : n));
          break;
        case 4:
          p += time_range(std::chrono::minutes(negate ? -n : n));
          break;
        case 5:
          p += time_range(std::chrono::hours(negate ? -n : n));
          break;
        case 6:
          p = p.delta(0, 0, 0, negate ? -n : n);
          break;
        case 7:
          p = p.delta(0, 0, 0, (negate ? -n : n) * 7);
          break;
        case 8:
          p = p.delta(0, 0, 0, 0, negate ? -n : n);
          break;
        case 9:
          p = p.delta(0, 0, 0, 0, 0, negate ? -n : n);
          break;
      }
    }

    vast::time_point& p;
  };

  time_point();

  qi::symbols<char> ns, us, ms, sec, min, hour, day, week, month, year;

  qi::rule<Iterator, vast::time_point(), skipper<Iterator>> time, delta;
  qi::rule<Iterator, vast::time_point()> fmt0, fmt1, fmt2, fmt3, fmt4;
  qi::rule<Iterator> digit2, digit4;
  duration<Iterator> dur;

  boost::phoenix::function<absolute_time> at;
  boost::phoenix::function<initializer> init;
  boost::phoenix::function<adder> add;

  vast::time_point point;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
