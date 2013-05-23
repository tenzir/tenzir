#ifndef VAST_DETAIL_PARSER_TIME_POINT_DEFINITION_H
#define VAST_DETAIL_PARSER_TIME_POINT_DEFINITION_H

#include "vast/detail/parser/time_point.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
time_point<Iterator>::time_point()
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

  ns.add
    ("n")
    ("ns")
    ("nsec")
    ("nsecs")
    ;

  us.add
    ("u")
    ("mu")
    ("musec")
    ("musecs")
    ("i")
    ;

  ms.add
    ("ms")
    ("msec")
    ("msecs")
    ;

  sec.add
    ("s")
    ("sec")
    ("secs")
    ;

  min.add
    ("m")
    ("min")
    ("mins")
    ;

  hour.add
    ("h")
    ("hour")
    ("hours")
    ;

  day.add
    ("d")
    ("day")
    ("days")
    ;

  week.add
    ("W")
    ("w")
    ("week")
    ("weeks")
    ;

  month.add
    ("M")
    ("mo")
    ("month")
    ("months")
    ;

  year.add
    ("Y")
    ("y")
    ("year")
    ("years")
    ;

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
    =   (long_long >> ns)       [add(0, _1, ref(negate))]
    |   (long_long >> us)       [add(1, _1, ref(negate))]
    |   (long_long >> ms)       [add(2, _1, ref(negate))]
    |   (long_long >> sec)      [add(3, _1, ref(negate))]
    |   (long_long >> min)      [add(4, _1, ref(negate))]
    |   (long_long >> hour)     [add(5, _1, ref(negate))]
    |   (long_long >> day)      [add(6, _1, ref(negate))]
    |   (long_long >> week)     [add(7, _1, ref(negate))]
    |   (long_long >> month)    [add(8, _1, ref(negate))]
    |   (long_long >> year)     [add(9, _1, ref(negate))]
    ;

  // TODO: Merge all fmt* rule into a single one, probably needs the syntactic
  // "not" parser ("!").

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
        ]   [_val = construct<vast::time_point>(
              construct<std::string>(begin(_1), end(_1)), "%Y-%m-%d+%H:%M:%S")]
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
        ]   [_val = construct<vast::time_point>(
              construct<std::string>(begin(_1), end(_1)), "%Y-%m-%d+%H:%M")]
    ;

  // YYYY-MM-DD HH
  fmt2
    =   raw
        [       digit4 >> '-'
            >>  digit2 >> '-'
            >>  digit2 >> '+'
            >>  digit2
        ]   [_val = construct<vast::time_point>(
              construct<std::string>(begin(_1), end(_1)), "%Y-%m-%d+%H")]
    ;

  // YYYY-MM-DD
  fmt3
    =   raw
        [   digit4 >> '-' >>  digit2 >> '-' >>  digit2
        ]   [_val = construct<vast::time_point>(
              construct<std::string>(begin(_1), end(_1)), "%Y-%m-%d")]
    ;

  // YYYY-MM
  fmt4
    =   raw
        [   digit4 >> '-' >>  digit2
        ]   [_val = construct<vast::time_point>(
              construct<std::string>(begin(_1), end(_1)), "%Y-%m")]
    ;

  digit2
    = repeat(2)[digit]
    ;

  digit4
    = repeat(4)[digit]
    ;
}

} // namespace parser
} // namespace detail
} // namespace vast

#endif
