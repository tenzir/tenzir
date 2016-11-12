#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/time.hpp"

#define SUITE time
#include "test.hpp"

using namespace vast;
using namespace std::chrono;

TEST(printable) {
  CHECK_EQUAL(to_string(nanoseconds(42)), "+42ns");
  CHECK_EQUAL(to_string(microseconds(42)), "+42us");
  CHECK_EQUAL(to_string(milliseconds(42)), "+42ms");
  CHECK_EQUAL(to_string(seconds(42)), "+42s");
  CHECK_EQUAL(to_string(minutes(42)), "+42min");
  CHECK_EQUAL(to_string(hours(42)), "+42h");
}

TEST(parseable) {
  interval i;
  MESSAGE("nanoseconds");
  CHECK(parsers::interval("42 nsecs", i));
  CHECK(i == nanoseconds(42));
  CHECK(parsers::interval("43nsecs", i));
  CHECK(i == nanoseconds(43));
  CHECK(parsers::interval("44ns", i));
  CHECK(i == nanoseconds(44));
  MESSAGE("microseconds");
  CHECK(parsers::interval("42 usecs", i));
  CHECK(i == microseconds(42));
  CHECK(parsers::interval("43usecs", i));
  CHECK(i == microseconds(43));
  CHECK(parsers::interval("44us", i));
  CHECK(i == microseconds(44));
  MESSAGE("milliseconds");
  CHECK(parsers::interval("42 msecs", i));
  CHECK(i == milliseconds(42));
  CHECK(parsers::interval("43msecs", i));
  CHECK(i == milliseconds(43));
  CHECK(parsers::interval("44ms", i));
  CHECK(i == milliseconds(44));
  MESSAGE("seconds");
  CHECK(parsers::interval("-42 secs", i));
  CHECK(i == seconds(-42));
  CHECK(parsers::interval("-43secs", i));
  CHECK(i == seconds(-43));
  CHECK(parsers::interval("-44s", i));
  CHECK(i == seconds(-44));
  MESSAGE("minutes");
  CHECK(parsers::interval("-42 mins", i));
  CHECK(i == minutes(-42));
  CHECK(parsers::interval("-43min", i));
  CHECK(i == minutes(-43));
  CHECK(parsers::interval("44m", i));
  CHECK(i == minutes(44));
  MESSAGE("hours");
  CHECK(parsers::interval("42 hours", i));
  CHECK(i == hours(42));
  CHECK(parsers::interval("-43hrs", i));
  CHECK(i == hours(-43));
  CHECK(parsers::interval("44h", i));
  CHECK(i == hours(44));
// TODO
// MESSAGE("compound");
// CHECK(parsers::interval("5m99s", i));
// CHECK(i.count() == 399000000000ll);
  timestamp ts;
// FIXME
  MESSAGE("YYYY-MM-DD+HH:MM:SS");
  CHECK(parsers::timestamp("2012-08-12+23:55:04", ts));
//  CHECK(ts == point::utc(2012, 8, 12, 23, 55, 4));
  MESSAGE("YYYY-MM-DD+HH:MM");
  CHECK(parsers::timestamp("2012-08-12+23:55", ts));
//  CHECK(ts == point::utc(2012, 8, 12, 23, 55));
  MESSAGE("YYYY-MM-DD+HH");
  CHECK(parsers::timestamp("2012-08-12+23", ts));
//  CHECK(ts == point::utc(2012, 8, 12, 23));
  MESSAGE("YYYY-MM-DD");
  CHECK(parsers::timestamp("2012-08-12", ts));
//  CHECK(ts == point::utc(2012, 8, 12));
  MESSAGE("YYYY-MM");
  CHECK(parsers::timestamp("2012-08", ts));
//  CHECK(ts == point::utc(2012, 8));
  MESSAGE("UNIX epoch");
  CHECK(parsers::timestamp("@1444040673", ts));
  CHECK(ts.time_since_epoch() == seconds{1444040673});
  CHECK(parsers::timestamp("@1398933902.686337", ts));
  CHECK(ts.time_since_epoch() == double_seconds{1398933902.686337});
  MESSAGE("now");
  CHECK(parsers::timestamp("now", ts));
  CHECK(ts > timestamp::clock::now() - minutes{1});
  CHECK(ts < timestamp::clock::now() + minutes{1});
  CHECK(parsers::timestamp("now - 1m", ts));
  CHECK(ts < timestamp::clock::now());
  CHECK(parsers::timestamp("now + 1m", ts));
  CHECK(ts > timestamp::clock::now());
  MESSAGE("ago");
  CHECK(parsers::timestamp("10 days ago", ts));
  CHECK(ts < timestamp::clock::now());
  MESSAGE("in");
  CHECK(parsers::timestamp("in 1 year", ts));
  CHECK(ts > timestamp::clock::now());
}

