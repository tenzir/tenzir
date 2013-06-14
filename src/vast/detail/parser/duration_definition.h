#ifndef VAST_DETAIL_PARSER_DURATION_DEFINITION_H
#define VAST_DETAIL_PARSER_DURATION_DEFINITION_H

#include "vast/detail/parser/duration.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
duration<Iterator>::duration()
  : duration::base_type(dur)
{
  qi::_val_type _val;
  qi::_1_type _1;
  qi::_2_type _2;
  qi::raw_type raw;
  qi::long_long_type num;

  unit.add
    ("n")
    ("ns")
    ("nsec")
    ("nsecs")
    ("u")
    ("mu")
    ("musec")
    ("musecs")
    ("i")
    ("ms")
    ("msec")
    ("msecs")
    ("s")
    ("sec")
    ("secs")
    ("m")
    ("min")
    ("mins")
    ("h")
    ("hour")
    ("hours")
    ("d")
    ("day")
    ("days")
    ("W")
    ("w")
    ("week")
    ("weeks")
    ("M")
    ("mo")
    ("month")
    ("months")
    ("Y")
    ("y")
    ("year")
    ("years")
    ;

  dur
      =   +((num >> raw[unit])     [_val += to_nano(_1, _2)])
      ;
}

} // namespace parser
} // namespace detail
} // namespace vast

#endif
