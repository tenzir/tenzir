#ifndef VAST_DETAIL_PARSER_TIME_DURATION_H
#define VAST_DETAIL_PARSER_TIME_DURATION_H

#include <string>
#include "vast/time.h"
#include "vast/detail/parser/boost.h"
#include "vast/detail/parser/skipper.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct time_duration
  : qi::grammar<Iterator, vast::time_duration(), skipper<Iterator>>
{
  struct nanoseconds_converter
  {
    template <typename, typename>
    struct result
    {
      using type = vast::time_duration;
    };

    template <typename Range>
    vast::time_duration operator()(int64_t d, Range r) const
    {
      std::string s(boost::begin(r), boost::end(r));
      if (s == "nsec" || s == "nsecs" || s == "ns" || s == "n")
        return vast::time_duration(std::chrono::nanoseconds(d));
      else if (s == "musec" || s == "musecs" || s == "mu" || s == "u")
        return vast::time_duration(std::chrono::microseconds(d));
      else if (s == "msec" || s == "msecs" || s == "ms")
        return vast::time_duration(std::chrono::milliseconds(d));
      else if (s == "sec" || s == "secs" || s == "s")
        return vast::time_duration(std::chrono::seconds(d));
      else if (s == "min" || s == "mins" || s == "m")
        return vast::time_duration(std::chrono::minutes(d));
      else if (s == "hour" || s == "hours" || s == "h")
        return vast::time_duration(std::chrono::hours(d));
      else if (s == "day" || s == "days" || s == "d")
        return vast::time_duration(
            std::chrono::duration<int64_t, std::ratio<86400>>(d));
      else if (s == "week" || s == "weeks" || s == "w" || s == "W")
        return vast::time_duration(
            std::chrono::duration<int64_t, std::ratio<604800>>(d));
      else if (s == "month" || s == "months" || s == "mo" || s == "M")
        return vast::time_duration(
            std::chrono::duration<int64_t, std::ratio<2592000>>(d));
      else if (s == "year" || s == "years" || s == "y" || s == "Y")
        return vast::time_duration(
            std::chrono::duration<int64_t, std::ratio<31536000>>(d));

      assert(! "missing cast implementation");
    }
  };

  time_duration()
    : time_duration::base_type(dur)
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

  qi::symbols<char> unit;
  qi::rule<Iterator, vast::time_duration(), skipper<Iterator>> dur;
  boost::phoenix::function<nanoseconds_converter> to_nano;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
