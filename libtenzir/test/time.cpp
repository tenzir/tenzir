//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/time.hpp"

#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/std/chrono.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/time.hpp"

#include <chrono>
#include <cstdlib>
#include <ctime>

using namespace tenzir;
using namespace std::chrono;
using namespace std::chrono_literals;

namespace {

template <class Input, class T>
void check_duration(const Input& str, T x) {
  tenzir::duration t;
  CHECK(parsers::duration(str, t));
  CHECK_EQUAL(t, duration_cast<tenzir::duration>(x));
}

} // namespace

TEST(positive durations) {
  MESSAGE("nanoseconds");
  check_duration("42 nanoseconds", 42ns);
  check_duration("42 nanosecond", 42ns);
  check_duration("42 nsecs", 42ns);
  check_duration("42nsec", 42ns);
  check_duration("42ns", 42ns);
  MESSAGE("microseconds");
  check_duration("42 microseconds", 42us);
  check_duration("42 microsecond", 42us);
  check_duration("42 usecs", 42us);
  check_duration("42usec", 42us);
  check_duration("42us", 42us);
  MESSAGE("milliseconds");
  check_duration("42 milliseconds", 42ms);
  check_duration("42 millisecond", 42ms);
  check_duration("42 msecs", 42ms);
  check_duration("42msec", 42ms);
  check_duration("42ms", 42ms);
  MESSAGE("seconds");
  check_duration("42 seconds", 42s);
  check_duration("42 second", 42s);
  check_duration("42 secs", 42s);
  check_duration("42sec", 42s);
  check_duration("42s", 42s);
  MESSAGE("minutes");
  check_duration("42 minutes", 42min);
  check_duration("42 minute", 42min);
  check_duration("42 mins", 42min);
  check_duration("42min", 42min);
  check_duration("42m", 42min);
  MESSAGE("hours");
  check_duration("42 hours", 42h);
  check_duration("42hour", 42h);
  check_duration("42h", 42h);
  MESSAGE("weeks");
  check_duration("1 weeks", 168h);
  check_duration("1week", 168h);
  check_duration("1w", 168h);
  MESSAGE("years");
  check_duration("1 years", 8760h);
  check_duration("1year", 8760h);
  check_duration("1y", 8760h);
}

TEST(negative durations) {
  check_duration("-42ns", -42ns);
  check_duration("-42h", -42h);
}

TEST(fractional durations) {
  check_duration("3.54s", 3540ms);
  check_duration("-42.001ms", -42001us);
}

TEST(compound durations) {
  check_duration("3m42s10ms", 3min + 42s + 10ms);
  check_duration("3s42s10ms", 3s + 42s + 10ms);
  check_duration("42s3m10ms", 3min + 42s + 10ms);
  check_duration("-10m8ms1ns", -(10min + 8ms + 1ns));
  MESSAGE("no intermediate signs");
  auto p = parsers::duration >> parsers::eoi;
  CHECK(!p("-10m-8ms1ns"));
}

bool verify_date(tenzir::time ts, int y, int m, int d) {
  auto time = system_clock::to_time_t(
    std::chrono::time_point_cast<system_clock::duration>(ts));
  std::tm tm = {};
  if (nullptr == gmtime_r(&time, &tm))
    return false;
  return tm.tm_year + 1900 == y && tm.tm_mon + 1 == m && tm.tm_mday == d;
}

tenzir::duration to_hours(tenzir::duration ts) {
  return duration_cast<hours>(ts) % 24;
}

tenzir::duration to_minutes(tenzir::duration ts) {
  return duration_cast<minutes>(ts) % 60;
}

tenzir::duration to_seconds(tenzir::duration ts) {
  return duration_cast<seconds>(ts) % 60;
}

tenzir::duration to_microseconds(tenzir::duration ts) {
  return duration_cast<microseconds>(ts) % 1'000'000;
}

TEST(ymdshms time parser) {
  tenzir::time ts;
  MESSAGE("YYYY-MM-DD+HH:MM:SS.ssss+HH");
  CHECK(parsers::time("2012-08-12+23:55:04.001234-01", ts));
  auto sd = floor<tenzir::days>(ts);
  auto t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 13));
  CHECK(to_hours(t) == hours{0});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{4});
  CHECK(to_microseconds(t) == microseconds{1234});
  MESSAGE("YYYY-MM-DD+HH:MM:SS.ssss");
  CHECK(parsers::time("2012-08-12+23:55:04.001234", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 12));
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{4});
  CHECK(to_microseconds(t) == microseconds{1234});
  MESSAGE("YYYY-MM-DD+HH:MM:SS-HH:MM");
  CHECK(parsers::time("2012-08-12+23:55:04+00:30", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 12));
  CHECK_EQUAL(to_hours(t), hours{23});
  CHECK_EQUAL(to_minutes(t), minutes{25});
  CHECK(to_seconds(t) == seconds{4});
  MESSAGE("YYYY-MM-DD+HH:MM:SS");
  CHECK(parsers::time("2012-08-12+23:55:04", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 12));
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{4});
  MESSAGE("YYYY-MM-DD HH:MM:SS"); // space as delimiter; needed for Sysmon
  CHECK(parsers::time("2012-08-12 23:55:04", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 12));
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{4});
  // TODO: Fix timezone offset without divider
  MESSAGE("YYYY-MM-DD+HH:MM+HHMM");
  CHECK(parsers::time("2012-08-12+23:55-0130", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 13));
  CHECK_EQUAL(to_hours(t), hours{1});
  CHECK_EQUAL(to_minutes(t), minutes{25});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM-DD+HH:MM");
  CHECK(parsers::time("2012-08-12+23:55", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 12));
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{55});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM-DD+HH");
  CHECK(parsers::time("2012-08-12+23", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 12));
  CHECK(to_hours(t) == hours{23});
  CHECK(to_minutes(t) == minutes{0});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM-DD");
  CHECK(parsers::time("2012-08-12", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 12));
  CHECK(to_hours(t) == hours{0});
  CHECK(to_minutes(t) == minutes{0});
  CHECK(to_seconds(t) == seconds{0});
  MESSAGE("YYYY-MM");
  CHECK(parsers::time("2012-08", ts));
  sd = floor<tenzir::days>(ts);
  t = ts - sd;
  CHECK(verify_date(sd, 2012, 8, 1));
  CHECK(to_hours(t) == hours{0});
  CHECK(to_minutes(t) == minutes{0});
  CHECK(to_seconds(t) == seconds{0});
}

TEST(unix epoch time parser) {
  tenzir::time ts;
  CHECK(parsers::time("@1444040673", ts));
  CHECK(ts.time_since_epoch() == 1444040673s);
  CHECK(parsers::time("@1398933902.686337", ts));
  CHECK(ts.time_since_epoch() == double_seconds{1398933902.686337});
}

TEST(now time parser) {
  tenzir::time ts;
  CHECK(parsers::time("now", ts));
  CHECK(ts > time::clock::now() - minutes{1});
  CHECK(ts < time::clock::now() + minutes{1});
  CHECK(parsers::time("now - 1m", ts));
  CHECK(ts < time::clock::now());
  CHECK(parsers::time("now + 1m", ts));
  CHECK(ts > time::clock::now());
}

TEST(ago time parser) {
  tenzir::time ts;
  CHECK(parsers::time("10 days ago", ts));
  CHECK(ts < time::clock::now());
}

TEST(in time parser) {
  tenzir::time ts;
  CHECK(parsers::time("in 1 year", ts));
  CHECK(ts > time::clock::now());
}
