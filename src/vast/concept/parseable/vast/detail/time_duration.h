#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_TIME_DURATION_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_TIME_DURATION_H

#include <string>

#include "vast/time.h"
#include "vast/concept/parseable/vast/detail/boost.h"
#include "vast/concept/parseable/vast/detail/skipper.h"
#include "vast/util/assert.h"

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunsequenced"
#endif

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct time_duration
  : qi::grammar<Iterator, time::duration(), skipper<Iterator>> {
  struct nanoseconds_converter {
    template <typename, typename>
    struct result {
      using type = time::duration;
    };

    template <typename Range>
    time::duration operator()(int64_t d, Range r) const {
      std::string s(boost::begin(r), boost::end(r));
      if (s == "nsec" || s == "nsecs" || s == "ns" || s == "n")
        return time::nanoseconds(d);
      else if (s == "musec" || s == "musecs" || s == "mu" || s == "u")
        return time::microseconds(d);
      else if (s == "msec" || s == "msecs" || s == "ms")
        return time::milliseconds(d);
      else if (s == "sec" || s == "secs" || s == "s")
        return time::seconds(d);
      else if (s == "min" || s == "mins" || s == "m")
        return time::minutes(d);
      else if (s == "hour" || s == "hours" || s == "h")
        return time::hours(d);
      else if (s == "day" || s == "days" || s == "d")
        return time::duration(
          std::chrono::duration<int64_t, std::ratio<86400>>(d));
      else if (s == "week" || s == "weeks" || s == "w" || s == "W")
        return time::duration(
          std::chrono::duration<int64_t, std::ratio<604800>>(d));
      else if (s == "month" || s == "months" || s == "mo" || s == "M")
        return time::duration(
          std::chrono::duration<int64_t, std::ratio<2592000>>(d));
      else if (s == "year" || s == "years" || s == "y" || s == "Y")
        return time::duration(
          std::chrono::duration<int64_t, std::ratio<31536000>>(d));
      VAST_ASSERT(!"missing cast implementation");
      return {};
    }
  };

  struct epoch_converter {
    template <typename>
    struct result {
      using type = time::duration;
    };

    time::duration operator()(double d) const {
      return time::fractional(d);
    }
  };

  time_duration() : time_duration::base_type(dur) {
    qi::_val_type _val;
    qi::_1_type _1;
    qi::_2_type _2;
    qi::raw_type raw;
    qi::long_long_type count;
    qi::real_parser<double, qi::strict_real_policies<double>> real;

    ns = "n", "ns", "nsec", "nsecs";
    us = "u", "mu", "musec", "musecs", "i";
    ms = "ms", "msec", "msecs";
    sec = "s", "sec", "secs";
    min = "m", "min", "mins";
    hour = "h", "hour", "hours";
    day = "d", "day", "days";
    week = "W", "w", "week", "weeks";
    month = "M", "mo", "month", "months";
    year = "Y", "y", "year", "years";

    dur
        =   +(  (real >> sec)           [_val += to_epoch(_1)]
             |  (count >> raw[units])   [_val += to_nano(_1, _2)]
             )
        ;

    units
        =   ns
        |   us
        |   ms
        |   sec
        |   month
        |   min
        |   hour
        |   day
        |   week
        |   year
        ;
  }

  qi::symbols<char> ns, us, ms, sec, min, hour, day, week, month, year;
  qi::rule<Iterator, time::duration(), skipper<Iterator>> dur;
  qi::rule<Iterator, skipper<Iterator>> units;
  boost::phoenix::function<nanoseconds_converter> to_nano;
  boost::phoenix::function<epoch_converter> to_epoch;
};

} // namespace parser
} // namespace detail
} // namespace vast

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

#endif
