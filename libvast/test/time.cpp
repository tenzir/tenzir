/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/time.hpp"

#define SUITE time
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::chrono;
using namespace std::chrono_literals;

namespace {

template <class Input, class T>
void check_timespan(const Input& str, T x) {
  timespan t;
  CHECK(parsers::timespan(str, t));
  CHECK_EQUAL(t, duration_cast<timespan>(x));
}

} // namespace <anonymous>

TEST(positive durations) {
  MESSAGE("nanoseconds");
  check_timespan("42 nsecs", 42ns);
  check_timespan("42nsec", 42ns);
  check_timespan("42ns", 42ns);
  check_timespan("42ns", 42ns);
  MESSAGE("microseconds");
  check_timespan("42 usecs", 42us);
  check_timespan("42usec", 42us);
  check_timespan("42us", 42us);
  MESSAGE("milliseconds");
  check_timespan("42 msecs", 42ms);
  check_timespan("42msec", 42ms);
  check_timespan("42ms", 42ms);
  MESSAGE("seconds");
  check_timespan("42 secs", 42s);
  check_timespan("42sec", 42s);
  check_timespan("42s", 42s);
  MESSAGE("minutes");
  check_timespan("42 mins", 42min);
  check_timespan("42min", 42min);
  check_timespan("42m", 42min);
  MESSAGE("hours");
  check_timespan("42 hours", 42h);
  check_timespan("42hour", 42h);
  check_timespan("42h", 42h);
}

TEST(negative durations) {
  check_timespan("-42ns", -42ns);
  check_timespan("-42h", -42h);
}

TEST(fractional durations) {
  check_timespan("3.54s", 3540ms);
  check_timespan("-42.001ms", -42001us);
}

TEST(compound durations) {
  check_timespan("3m42s10ms", 3min + 42s + 10ms);
  check_timespan("3s42s10ms", 3s + 42s + 10ms);
  check_timespan("42s3m10ms", 3min + 42s + 10ms);
  check_timespan("-10m8ms1ns", -10min + 8ms + 1ns);
  MESSAGE("no intermediate signs");
  auto p = parsers::timespan >> parsers::eoi;
  CHECK(!p("-10m-8ms1ns"));
}

timespan to_hours(timespan ts) {
  return duration_cast<hours>(ts) % 24;
}

timespan to_minutes(timespan ts) {
  return duration_cast<minutes>(ts) % 60;
}

timespan to_seconds(timespan ts) {
  return duration_cast<seconds>(ts) % 60;
}

timespan to_microseconds(timespan ts) {
  return duration_cast<microseconds>(ts) % 1'000'000;
}

TEST(ymdshms timestamp parser) {
  timestamp ts;
  MESSAGE("YYYY-MM-DD+HH:MM:SS.ssss+HH");
  CHECK(parsers::timestamp("2012-08-12+23:55:04.001234+01", ts));
  auto sd = floor<days>(ts);
  auto t = ts - sd;
  CHECK(sd == years{2012} / 8 / 13);
  CHECK(to_hours(t) == hours{0});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{4});
  CHECK(to_microseconds(t) == microseconds{1234});
  MESSAGE("YYYY-MM-DD+HH:MM:SS.ssss");
  CHECK(parsers::timestamp("2012-08-12+23:55:04.001234", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 12);
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{4});
  CHECK(to_microseconds(t) == microseconds{1234});
  MESSAGE("YYYY-MM-DD+HH:MM:SS-HH:MM");
  CHECK(parsers::timestamp("2012-08-12+23:55:04-00:30", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 12);
  CHECK_EQUAL(to_hours(t), hours{23});
  CHECK_EQUAL(to_minutes(t), minutes{25});
  CHECK(to_seconds(t) == seconds{4});
  MESSAGE("YYYY-MM-DD+HH:MM:SS");
  CHECK(parsers::timestamp("2012-08-12+23:55:04", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 12);
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{4});
  // TODO: Fix timezone offset without divider
  MESSAGE("YYYY-MM-DD+HH:MM+HHMM");
  CHECK(parsers::timestamp("2012-08-12+23:55+0130", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 13);
  CHECK_EQUAL(to_hours(t), hours{1});
  CHECK_EQUAL(to_minutes(t), minutes{25});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM-DD+HH:MM");
  CHECK(parsers::timestamp("2012-08-12+23:55", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 12);
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM-DD+HH");
  CHECK(parsers::timestamp("2012-08-12+23", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 12);
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{0});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM-DD");
  CHECK(parsers::timestamp("2012-08-12", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 12);
  CHECK(to_hours(t) == hours{0});
  CHECK(to_minutes(t) == minutes{0});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM");
  CHECK(parsers::timestamp("2012-08", ts));
  sd = floor<days>(ts);
  t = ts - sd;
  CHECK(sd == years{2012} / 8 / 1);
  CHECK(to_hours(t) == hours{0});
  CHECK(to_minutes(t) == minutes{0});
  CHECK(to_seconds(t) == seconds{0});
}

TEST(unix epoch timestamp parser) {
  timestamp ts;
  CHECK(parsers::timestamp("@1444040673", ts));
  CHECK(ts.time_since_epoch() == 1444040673s);
  CHECK(parsers::timestamp("@1398933902.686337", ts));
  CHECK(ts.time_since_epoch() == double_seconds{1398933902.686337});
}

TEST(now timestamp parser) {
  timestamp ts;
  CHECK(parsers::timestamp("now", ts));
  CHECK(ts > timestamp::clock::now() - minutes{1});
  CHECK(ts < timestamp::clock::now() + minutes{1});
  CHECK(parsers::timestamp("now - 1m", ts));
  CHECK(ts < timestamp::clock::now());
  CHECK(parsers::timestamp("now + 1m", ts));
  CHECK(ts > timestamp::clock::now());
}

TEST(ago timestamp parser) {
  timestamp ts;
  CHECK(parsers::timestamp("10 days ago", ts));
  CHECK(ts < timestamp::clock::now());
}

TEST(in timestamp parser) {
  timestamp ts;
  CHECK(parsers::timestamp("in 1 year", ts));
  CHECK(ts > timestamp::clock::now());
}
