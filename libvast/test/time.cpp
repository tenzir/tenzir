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

#include <date/date.h>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/time.hpp"

#define SUITE time
#include "test.hpp"

using namespace vast;
using namespace std::chrono;
using namespace date;

TEST(parseable) {
  timespan sp;
  MESSAGE("nanoseconds");
  CHECK(parsers::timespan("42 nsecs", sp));
  CHECK(sp == nanoseconds(42));
  CHECK(parsers::timespan("43nsecs", sp));
  CHECK(sp == nanoseconds(43));
  CHECK(parsers::timespan("44ns", sp));
  CHECK(sp == nanoseconds(44));
  MESSAGE("microseconds");
  CHECK(parsers::timespan("42 usecs", sp));
  CHECK(sp == microseconds(42));
  CHECK(parsers::timespan("43usecs", sp));
  CHECK(sp == microseconds(43));
  CHECK(parsers::timespan("44us", sp));
  CHECK(sp == microseconds(44));
  MESSAGE("milliseconds");
  CHECK(parsers::timespan("42 msecs", sp));
  CHECK(sp == milliseconds(42));
  CHECK(parsers::timespan("43msecs", sp));
  CHECK(sp == milliseconds(43));
  CHECK(parsers::timespan("44ms", sp));
  CHECK(sp == milliseconds(44));
  MESSAGE("seconds");
  CHECK(parsers::timespan("-42 secs", sp));
  CHECK(sp == seconds(-42));
  CHECK(parsers::timespan("-43secs", sp));
  CHECK(sp == seconds(-43));
  CHECK(parsers::timespan("-44s", sp));
  CHECK(sp == seconds(-44));
  MESSAGE("minutes");
  CHECK(parsers::timespan("-42 mins", sp));
  CHECK(sp == minutes(-42));
  CHECK(parsers::timespan("-43min", sp));
  CHECK(sp == minutes(-43));
  CHECK(parsers::timespan("44m", sp));
  CHECK(sp == minutes(44));
  MESSAGE("hours");
  CHECK(parsers::timespan("42 hours", sp));
  CHECK(sp == hours(42));
  CHECK(parsers::timespan("-43hrs", sp));
  CHECK(sp == hours(-43));
  CHECK(parsers::timespan("44h", sp));
  CHECK(sp == hours(44));
// TODO
// MESSAGE("compound");
// CHECK(parsers::timespan("5m99s", sp));
// CHECK(sp.count() == 399000000000ll);
  timestamp ts;
  MESSAGE("YYYY-MM-DD+HH:MM:SS");
  CHECK(parsers::timestamp("2012-08-12+23:55:04", ts));
  auto sd = floor<days>(ts);
  auto t = make_time(ts - sd);
  CHECK(sd == 2012_y/8/12);
  CHECK(t.hours() == hours{23});
  CHECK(t.minutes() == minutes{55});
  CHECK(t.seconds() == seconds{4});
  MESSAGE("YYYY-MM-DD+HH:MM");
  CHECK(parsers::timestamp("2012-08-12+23:55", ts));
  sd = floor<days>(ts);
  t = make_time(ts - sd);
  CHECK(sd == 2012_y/8/12);
  CHECK(t.hours() == hours{23});
  CHECK(t.minutes() == minutes{55});
  CHECK(t.seconds() == seconds{0});
  MESSAGE("YYYY-MM-DD+HH");
  CHECK(parsers::timestamp("2012-08-12+23", ts));
  sd = floor<days>(ts);
  t = make_time(ts - sd);
  CHECK(sd == 2012_y/8/12);
  CHECK(t.hours() == hours{23});
  CHECK(t.minutes() == minutes{0});
  CHECK(t.seconds() == seconds{0});
  MESSAGE("YYYY-MM-DD");
  CHECK(parsers::timestamp("2012-08-12", ts));
  sd = floor<days>(ts);
  t = make_time(ts - sd);
  CHECK(sd == 2012_y/8/12);
  CHECK(t.hours() == hours{0});
  CHECK(t.minutes() == minutes{0});
  CHECK(t.seconds() == seconds{0});
  MESSAGE("YYYY-MM");
  CHECK(parsers::timestamp("2012-08", ts));
  sd = floor<days>(ts);
  t = make_time(ts - sd);
  CHECK(sd == 2012_y/8/1);
  CHECK(t.hours() == hours{0});
  CHECK(t.minutes() == minutes{0});
  CHECK(t.seconds() == seconds{0});
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

