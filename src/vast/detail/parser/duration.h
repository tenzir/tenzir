#ifndef VAST_DETAIL_PARSER_DURATION_H
#define VAST_DETAIL_PARSER_DURATION_H

#include <string>
#include "vast/time.h"
#include "vast/detail/parser/boost.h"
#include "vast/detail/parser/skipper.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct duration : qi::grammar<Iterator, time_range(), skipper<Iterator>>
{
  struct nanoseconds_converter
  {
    template <typename, typename>
    struct result
    {
      typedef time_range type;
    };

    template <typename Range>
    time_range operator()(int64_t d, Range r) const
    {
      std::string s(begin(r), end(r));
      if (s == "nsec" || s == "nsecs" || s == "ns" || s == "n")
        return time_range(std::chrono::nanoseconds(d));
      else if (s == "musec" || s == "musecs" || s == "mu" || s == "u")
        return time_range(std::chrono::microseconds(d));
      else if (s == "msec" || s == "msecs" || s == "ms")
        return time_range(std::chrono::milliseconds(d));
      else if (s == "sec" || s == "secs" || s == "s")
        return time_range(std::chrono::seconds(d));
      else if (s == "min" || s == "mins" || s == "m")
        return time_range(std::chrono::minutes(d));
      else if (s == "hour" || s == "hours" || s == "h")
        return time_range(std::chrono::hours(d));
      else if (s == "day" || s == "days" || s == "d")
        return time_range(
            std::chrono::duration<int64_t, std::ratio<86400>>(d));
      else if (s == "week" || s == "weeks" || s == "w" || s == "W")
        return time_range(
            std::chrono::duration<int64_t, std::ratio<604800>>(d));
      else if (s == "month" || s == "months" || s == "mo" || s == "M")
        return time_range(
            std::chrono::duration<int64_t, std::ratio<2592000>>(d));
      else if (s == "year" || s == "years" || s == "y" || s == "Y")
        return time_range(
            std::chrono::duration<int64_t, std::ratio<31536000>>(d));

      assert(! "missing cast implementation");
    }
  };

  duration();

  qi::symbols<char> unit;
  qi::rule<Iterator, time_range(), skipper<Iterator>> dur;
  boost::phoenix::function<nanoseconds_converter> to_nano;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
