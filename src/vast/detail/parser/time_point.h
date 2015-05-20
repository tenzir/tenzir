#ifndef VAST_DETAIL_PARSER_TIME_POINT_H
#define VAST_DETAIL_PARSER_TIME_POINT_H

#include "vast/detail/parser/time_duration.h"
#include "vast/util/assert.h"

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunsequenced"
#endif

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct time_point : qi::grammar<Iterator, time::point(), skipper<Iterator>>
{
  struct absolute_time
  {
    template <typename>
    struct result
    {
      typedef time::point type;
    };

    time::point operator()(time::duration d) const
    {
      return time::point{} + d;
    }
  };

  struct initializer
  {
    initializer(time::point& p)
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
      p = time::now();
    }

    time::point& p;
  };

  struct adder
  {
    adder(time::point& p)
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
          VAST_ASSERT(! "invalid tag");
          break;
        case 0:
          p += time::duration(std::chrono::nanoseconds(negate ? -n : n));
          break;
        case 1:
          p += time::duration(std::chrono::microseconds(negate ? -n : n));
          break;
        case 2:
          p += time::duration(std::chrono::milliseconds(negate ? -n : n));
          break;
        case 3:
          p += time::duration(std::chrono::seconds(negate ? -n : n));
          break;
        case 4:
          p += time::duration(std::chrono::minutes(negate ? -n : n));
          break;
        case 5:
          p += time::duration(std::chrono::hours(negate ? -n : n));
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

    time::point& p;
  };

  struct time_point_factory
  {
    template <typename, typename, typename>
    struct result
    {
      using type = time::point;
    };

    template <typename I>
    time::point operator()(I begin, I end, char const* fmt) const
    {
      auto t = parse<time::point>(begin, end, fmt);
      return t ? *t : time::point{};
    }
  };

  time_point()
    : time_point::base_type(time),
      init(point),
      add(point)
  {
    using boost::phoenix::construct;
    using boost::phoenix::begin;
    using boost::phoenix::end;
    using boost::phoenix::ref;

    qi::_1_type _1;
    qi::_val_type _val;
    qi::raw_type raw;
    qi::lit_type lit;
    qi::digit_type digit;
    qi::long_long_type long_long;
    qi::repeat_type repeat;

    bool negate = false;
    time
      =   (   lit("now")  [init(0)]
          >>  -(  ( lit('+')  [ref(negate) = false]
                  | lit('-')  [ref(negate) = true]
                  )
                  >>  +delta
               )
          )               [_val = ref(point)]
      |   ('@' >> dur)    [_val = at(_1)]
      |   fmt0            [_val = _1]
      |   fmt1            [_val = _1]
      |   fmt2            [_val = _1]
      |   fmt3            [_val = _1]
      |   fmt4            [_val = _1]
      ;

    delta
      =   (long_long >> dur.ns)       [add(0, _1, ref(negate))]
      |   (long_long >> dur.us)       [add(1, _1, ref(negate))]
      |   (long_long >> dur.ms)       [add(2, _1, ref(negate))]
      |   (long_long >> dur.sec)      [add(3, _1, ref(negate))]
      |   (long_long >> dur.min)      [add(4, _1, ref(negate))]
      |   (long_long >> dur.hour)     [add(5, _1, ref(negate))]
      |   (long_long >> dur.day)      [add(6, _1, ref(negate))]
      |   (long_long >> dur.week)     [add(7, _1, ref(negate))]
      |   (long_long >> dur.month)    [add(8, _1, ref(negate))]
      |   (long_long >> dur.year)     [add(9, _1, ref(negate))]
      ;

    // TODO: Merge all fmt* rule into a single one, probably needs the
    // syntactic "not" parser ("!").

    // YYYY-MM-DD HH:MM:SS
    fmt0
      =   raw
          [       digit4 >> '-'
              >>  digit2 >> '-'
              >>  digit2 >> '+'
              >>  digit2
              >>  ':'
              >>  digit2 >> ':'
              >>  digit2
          ]   [_val = make_time_point(begin(_1), end(_1), "%Y-%m-%d+%H:%M:%S")]
      ;

    // YYYY-MM-DD HH:MM
    fmt1
      =   raw
          [       digit4 >> '-'
              >>  digit2 >> '-'
              >>  digit2 >> '+'
              >>  digit2
              >>  ':'
              >>  digit2
          ]   [_val = make_time_point(begin(_1), end(_1), "%Y-%m-%d+%H:%M")]
      ;

    // YYYY-MM-DD HH
    fmt2
      =   raw
          [       digit4 >> '-'
              >>  digit2 >> '-'
              >>  digit2 >> '+'
              >>  digit2
          ]   [_val = make_time_point(begin(_1), end(_1), "%Y-%m-%d+%H")]
      ;

    // YYYY-MM-DD
    fmt3
      =   raw
          [   digit4 >> '-' >>  digit2 >> '-' >>  digit2
          ]   [_val = make_time_point(begin(_1), end(_1), "%Y-%m-%d")]
      ;

    // YYYY-MM
    fmt4
      =   raw
          [   digit4 >> '-' >>  digit2
          ]   [_val = make_time_point(begin(_1), end(_1), "%Y-%m")]
      ;

    digit2
      = repeat(2)[digit]
      ;

    digit4
      = repeat(4)[digit]
      ;
  }

  qi::rule<Iterator, time::point(), skipper<Iterator>> time, delta;
  qi::rule<Iterator, time::point()> fmt0, fmt1, fmt2, fmt3, fmt4;
  qi::rule<Iterator> digit2, digit4;
  time_duration<Iterator> dur;

  boost::phoenix::function<absolute_time> at;
  boost::phoenix::function<initializer> init;
  boost::phoenix::function<adder> add;
  boost::phoenix::function<time_point_factory> make_time_point;

  time::point point;
};

} // namespace parser
} // namespace detail
} // namespace vast

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

#endif
