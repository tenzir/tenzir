#ifndef VAST_CONCEPT_PARSEABLE_VAST_TIME_H
#define VAST_CONCEPT_PARSEABLE_VAST_TIME_H

#include "vast/access.h"
#include "vast/time.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric/real.h"
#include "vast/concept/parseable/string/char_class.h"

namespace vast {

struct time_duration_parser : parser<time_duration_parser> {
  using attribute = time::duration;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    using namespace parsers;
    auto save = f;
    int64_t i;
    if (!i64.parse(f, l, i))
      return false;
    static auto whitespace = *blank;
    if (!whitespace.parse(f, l, unused)) {
      f = save;
      return false;
    }
    static auto unit
      = "nsecs"_p ->* [] { return time::nanoseconds(1); }
      | "nsec"_p  ->* [] { return time::nanoseconds(1); }
      | "ns"_p    ->* [] { return time::nanoseconds(1); }
      | "usecs"_p ->* [] { return time::microseconds(1); }
      | "usec"_p  ->* [] { return time::microseconds(1); }
      | "us"_p    ->* [] { return time::microseconds(1); }
      | "msecs"_p ->* [] { return time::milliseconds(1); }
      | "msec"_p  ->* [] { return time::milliseconds(1); }
      | "ms"_p    ->* [] { return time::milliseconds(1); }
      | "secs"_p  ->* [] { return time::seconds(1); }
      | "sec"_p   ->* [] { return time::seconds(1); }
      | "s"_p     ->* [] { return time::seconds(1); }
      | "mins"_p  ->* [] { return time::minutes(1); }
      | "min"_p   ->* [] { return time::minutes(1); }
      | "m"_p     ->* [] { return time::minutes(1); }
      | "hours"_p ->* [] { return time::hours(1); }
      | "hrs"_p   ->* [] { return time::hours(1); }
      | "h"_p     ->* [] { return time::hours(1); }
      | "days"_p  ->* [] { return time::hours(24); }
      | "d"_p     ->* [] { return time::hours(24); }
      | "weeks"_p ->* [] { return time::hours(24 * 7); }
      | "w"_p     ->* [] { return time::hours(24 * 7); }
      | "years"_p ->* [] { return time::hours(24 * 365); }
      | "y"_p     ->* [] { return time::hours(24 * 365); }
      ;
    if (! unit.parse(f, l, a)) {
      f = save;
      return false;
    }
    a *= i;
    return true;
  }
};

template <>
struct parser_registry<time::duration> {
  using type = time_duration_parser;
};

namespace detail {

struct ymd_parser : vast::parser<ymd_parser> {
  using attribute = std::tm;

  static auto make() {
    auto year = integral_parser<unsigned, 4, 4>{};
    auto mon = integral_parser<unsigned, 2, 2>{};
    auto day = integral_parser<unsigned, 2, 2>{};
    return year >> '-' >> mon >> '-' >> day;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, std::tm& a) const {
    static auto p = make();
    unsigned y, m, d;
    auto t = std::tie(y, m, d);
    if (!p.parse(f, l, t))
      return false;
    a.tm_year = y - 1900;
    a.tm_mon = m - 1;
    a.tm_mday = d;
    return true;
  }
};

struct hms_parser : vast::parser<hms_parser> {
  using attribute = std::tm;

  static auto make() {
    auto hour = integral_parser<unsigned, 2, 2>{};
    auto min = integral_parser<unsigned, 2, 2>{};
    auto sec = integral_parser<unsigned, 2, 2>{};
    return hour >> ':' >> min >> ':' >> sec;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, std::tm& a) const {
    static auto p = make();
    unsigned h, m, s;
    auto t = std::tie(h, m, s);
    if (!p.parse(f, l, t))
      return false;
    a.tm_hour = h;
    a.tm_min = m;
    a.tm_sec = s;
    return true;
  }
};

} // detail

struct time_point_parser : parser<time_point_parser> {
  using attribute = time::point;

  static auto make() {
    using namespace parsers;
    // auto delta = lit("now") >> ('+' | '-') >> time_duration_parser{};
    return detail::ymd_parser{} >> '+' >> detail::hms_parser{};
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    static auto p = make();
    std::tm tm;
    std::memset(&tm, 0, sizeof(tm));
    if (!p.parse(f, l, tm))
      return false;
    a = time::point::from_tm(tm);
    return true;
  }
};

template <>
struct parser_registry<time::point> {
  using type = time_point_parser;
};

namespace parsers {

static auto const time_point = make_parser<time::point>();
static auto const time_duration = make_parser<time::duration>();

} // namespace parsers

} // namespace vast

#endif
