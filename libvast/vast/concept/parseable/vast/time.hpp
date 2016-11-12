#ifndef VAST_CONCEPT_PARSEABLE_VAST_TIME_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_TIME_HPP

#include <chrono>
#include <ctime>

#include "vast/access.hpp"
#include "vast/time.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {

// TODO: replace with Howard Hinnant's TZ stuff and then move into
// vast/concept/parseable/std/chrono.

template <class Rep, class Period>
struct duration_parser : parser<duration_parser<Rep, Period>> {
  using duration_type = std::chrono::duration<Rep, Period>;
  using attribute = duration_type;

  template <class Duration>
  static auto cast(Duration d) {
    return std::chrono::duration_cast<duration_type>(d);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    using namespace parsers;
    auto save = f;
    Rep i;
    if (!make_parser<Rep>{}.parse(f, l, i))
      return false;
    static auto whitespace = *space;
    if (!whitespace.parse(f, l, unused)) {
      f = save;
      return false;
    }
    using namespace std::chrono;
    static auto unit
      = "nsecs"_p ->* [] { return cast(nanoseconds(1)); }
      | "nsec"_p  ->* [] { return cast(nanoseconds(1)); }
      | "ns"_p    ->* [] { return cast(nanoseconds(1)); }
      | "usecs"_p ->* [] { return cast(microseconds(1)); }
      | "usec"_p  ->* [] { return cast(microseconds(1)); }
      | "us"_p    ->* [] { return cast(microseconds(1)); }
      | "msecs"_p ->* [] { return cast(milliseconds(1)); }
      | "msec"_p  ->* [] { return cast(milliseconds(1)); }
      | "ms"_p    ->* [] { return cast(milliseconds(1)); }
      | "secs"_p  ->* [] { return cast(seconds(1)); }
      | "sec"_p   ->* [] { return cast(seconds(1)); }
      | "s"_p     ->* [] { return cast(seconds(1)); }
      | "mins"_p  ->* [] { return cast(minutes(1)); }
      | "min"_p   ->* [] { return cast(minutes(1)); }
      | "m"_p     ->* [] { return cast(minutes(1)); }
      | "hrs"_p   ->* [] { return cast(hours(1)); }
      | "hours"_p ->* [] { return cast(hours(1)); }
      | "hour"_p  ->* [] { return cast(hours(1)); }
      | "h"_p     ->* [] { return cast(hours(1)); }
      | "days"_p  ->* [] { return cast(hours(24)); }
      | "day"_p   ->* [] { return cast(hours(24)); }
      | "d"_p     ->* [] { return cast(hours(24)); }
      | "weeks"_p ->* [] { return cast(hours(24 * 7)); }
      | "week"_p  ->* [] { return cast(hours(24 * 7)); }
      | "w"_p     ->* [] { return cast(hours(24 * 7)); }
      | "years"_p ->* [] { return cast(hours(24 * 365)); }
      | "year"_p  ->* [] { return cast(hours(24 * 365)); }
      | "y"_p     ->* [] { return cast(hours(24 * 365)); }
      ;
    if (!unit.parse(f, l, a)) {
      f = save;
      return false;
    }
    a *= i;
    return true;
  }
};

template <class Rep, class Period>
struct parser_registry<std::chrono::duration<Rep, Period>> {
  using type = duration_parser<Rep, Period>;
};

namespace parsers {

template <class Rep, class Period>
auto const duration = duration_parser<Rep, Period>{};

auto const interval = duration<vast::interval::rep, vast::interval::period>;

} // namespace parsers

struct ymdhms_parser : vast::parser<ymdhms_parser> {
  using attribute = timestamp;

  static auto make() {
    auto year = integral_parser<int, 4, 4>{}
                  .with([](auto x) { return x >= 1900; })
                  ->* [](int y) { return y - 1900; };
    auto mon = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 1 && x <= 12; })
                  ->* [](int m) { return m - 1; };
    auto day = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 1 && x <= 31; });
    auto hour = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 0 && x <= 23; });
    auto min = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 0 && x <= 59; });
    auto sec = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 0 && x <= 60; }); // leap sec
    return year >> '-' >> mon
        >> ~('-' >> day >> ~('+' >> hour >> ~(':' >> min >> ~(':' >> sec))));
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, timestamp& tp) const {
    static auto p = make();
    std::tm tm;
    std::memset(&tm, 0, sizeof(tm));
    tm.tm_mday = 1;
    auto ms = std::tie(tm.tm_min, tm.tm_sec);
    auto hms = std::tie(tm.tm_hour, ms);
    auto dhms = std::tie(tm.tm_mday, hms);
    auto ymdhms = std::tie(tm.tm_year, tm.tm_mon, dhms);
    if (!p.parse(f, l, ymdhms))
      return false;
// TODO
//    if (!time_zone_set) {
//      std::lock_guard<std::mutex> lock{time_zone_mutex};
//      if (::setenv("TZ", "GMT", 1))
//        die("could not set timzone variable");
//      time_zone_set = true;
//    }
    auto t = std::mktime(&tm);
    if (t == -1)
      return false;
    tp = std::chrono::system_clock::from_time_t(t);
    return true;
  }
};

namespace parsers {

auto const ymdhms = ymdhms_parser{};

/// Parses a fractional seconds-timestamp as UNIX epoch.
auto const epoch = real_opt_dot
  ->* [](double d) { 
    using std::chrono::duration_cast; 
    return timestamp{duration_cast<vast::interval>(double_seconds{d})};
  };

} // namespace parsers

struct timestamp_parser : parser<timestamp_parser> {
  using attribute = timestamp;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    static auto plus = [](interval i) {
      return timestamp::clock::now() + i;
    };
    static auto minus = [](interval i) {
      return timestamp::clock::now() - i;
    };
    static auto ws = ignore(*parsers::space);
    static auto p
      = parsers::ymdhms
      | '@' >> parsers::epoch
      | "now" >> ws >> ( '+' >> ws >> parsers::interval ->* plus
                       | '-' >> ws >> parsers::interval ->* minus )
      | "now"_p ->* []() { return timestamp::clock::now(); }
      | "in" >> ws >> parsers::interval ->* plus
      | (parsers::interval ->* minus) >> ws >> "ago"
      ;
    return p.parse(f, l, a);
  }
};

template <>
struct parser_registry<timestamp> {
  using type = timestamp_parser;
};

namespace parsers {

static auto const timestamp = timestamp_parser{};

} // namespace parsers

} // namespace vast

#endif
