//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/time_zone.hpp"

#include "tenzir/test/test.hpp"

#include <chrono>
#include <cstdlib>
#include <string>

using namespace tenzir;
using namespace std::chrono_literals;
using std::chrono::January;
using std::chrono::July;
using std::chrono::March;
using std::chrono::October;
using std::chrono::seconds;
using std::chrono::sys_days;

namespace {

auto utc_at(sys_days day, tenzir::duration time_of_day) -> tenzir::time {
  return tenzir::time{day} + time_of_day;
}

auto local_at(sys_days day, tenzir::duration time_of_day)
  -> tenzir::local_time {
  return tenzir::local_time{day.time_since_epoch() + time_of_day};
}

} // namespace

TEST("time zones resolve IANA names") {
  auto berlin = TimeZone::locate("Europe/Berlin");
  REQUIRE(berlin);
  CHECK_EQUAL(berlin->name(), "Europe/Berlin");
  // Zones are interned, so lookups return the same object.
  CHECK_EQUAL(&*berlin, &*TimeZone::locate("Europe/Berlin"));
  CHECK_EQUAL(TimeZone::utc().name(), "UTC");
  CHECK(not TimeZone::locate("Europe/Atlantis"));
  CHECK(not TimeZone::locate(""));
  // The host-dependent `localtime` is not a zone name.
  CHECK(not TimeZone::locate("localtime"));
  // Paths and other non-names never reach the file system.
  CHECK(not TimeZone::locate("/etc/localtime"));
  CHECK(not TimeZone::locate("../zoneinfo/UTC"));
  CHECK(not TimeZone::locate("Europe/../UTC"));
  CHECK(not TimeZone::locate("Europe//Berlin"));
  CHECK(not TimeZone::locate("Europe/Berlin/"));
  CHECK(not TimeZone::locate("file:Europe/Berlin"));
  CHECK(not TimeZone::locate("Fixed/UTC+01:00"));
  CHECK(TimeZone::locate("America/Argentina/Buenos_Aires"));
  CHECK(TimeZone::locate("Etc/GMT+5"));
}

TEST("TZDIR overrides the time zone database directory") {
  auto const* previous = std::getenv("TZDIR");
  auto restore = Option<std::string>{};
  if (previous) {
    restore = std::string{previous};
  }
  // Zones are interned, so use one that no other test looks up.
  ::setenv("TZDIR", "/nonexistent", 1);
  CHECK(not TimeZone::locate("Pacific/Chatham"));
  // UTC is built in and resolves without a database.
  CHECK(TimeZone::locate("UTC"));
  if (restore) {
    ::setenv("TZDIR", restore->c_str(), 1);
  } else {
    ::unsetenv("TZDIR");
  }
  CHECK(TimeZone::locate("Pacific/Chatham"));
}

TEST("time zones convert between local and UTC time") {
  auto const& zurich = *TimeZone::locate("Europe/Zurich");
  auto const summer = sys_days{2025y / July / 4};
  auto const winter = sys_days{2025y / January / 15};
  CHECK_EQUAL(zurich.to_utc(local_at(summer, 12h + 250ms)),
              utc_at(summer, 10h + 250ms));
  CHECK_EQUAL(zurich.to_utc(local_at(winter, 12h)), utc_at(winter, 11h));
  CHECK_EQUAL(zurich.to_local(utc_at(summer, 10h + 250ms)).time_since_epoch(),
              local_at(summer, 12h + 250ms).time_since_epoch());
  CHECK_EQUAL(zurich.offset(utc_at(summer, 0h)), seconds{2h});
  CHECK_EQUAL(zurich.offset(utc_at(winter, 0h)), seconds{1h});
  auto const& utc = TimeZone::utc();
  CHECK_EQUAL(utc.to_utc(local_at(summer, 12h)), utc_at(summer, 12h));
  CHECK_EQUAL(utc.offset(utc_at(summer, 0h)), seconds{0});
}

TEST("time zones resolve repeated and skipped wall-clock times") {
  auto const& zurich = *TimeZone::locate("Europe/Zurich");
  // Clocks spring forward from 02:00 to 03:00, so 02:30 shifts to 03:30.
  auto const spring = sys_days{2025y / March / 30};
  CHECK_EQUAL(zurich.to_utc(local_at(spring, 2h + 30min)),
              utc_at(spring, 1h + 30min));
  // Clocks fall back from 03:00 to 02:00, so 02:30 happens twice and resolves
  // to the earlier instant.
  auto const autumn = sys_days{2025y / October / 26};
  CHECK_EQUAL(zurich.to_utc(local_at(autumn, 2h + 30min)),
              utc_at(autumn, 30min));
}

TEST("time zones report the next offset transition") {
  auto const& zurich = *TimeZone::locate("Europe/Zurich");
  auto const spring = sys_days{2025y / March / 30};
  auto spring_transition
    = zurich.next_transition(utc_at(sys_days{2025y / January / 1}, 0h));
  REQUIRE(spring_transition);
  CHECK_EQUAL(*spring_transition, utc_at(spring, 1h));
  // The transition itself does not count as the next one.
  auto autumn_transition = zurich.next_transition(*spring_transition);
  REQUIRE(autumn_transition);
  CHECK_EQUAL(*autumn_transition, utc_at(sys_days{2025y / October / 26}, 1h));
  CHECK(not TimeZone::utc().next_transition(utc_at(spring, 0h)));
}

TEST("the current time zone follows TZ") {
  auto const* previous = std::getenv("TZ");
  auto restore = Option<std::string>{};
  if (previous) {
    restore = std::string{previous};
  }
  ::setenv("TZ", ":Asia/Tokyo", 1);
  auto current = TimeZone::current();
  REQUIRE(current);
  CHECK_EQUAL(current->name(), "Asia/Tokyo");
  ::setenv("TZ", "/usr/share/zoneinfo/America/New_York", 1);
  current = TimeZone::current();
  REQUIRE(current);
  CHECK_EQUAL(current->name(), "America/New_York");
  // An empty `TZ` selects UTC, while an unset one falls back to the system.
  ::setenv("TZ", "", 1);
  current = TimeZone::current();
  REQUIRE(current);
  CHECK_EQUAL(current->name(), "UTC");
  if (restore) {
    ::setenv("TZ", restore->c_str(), 1);
  } else {
    ::unsetenv("TZ");
  }
}
