//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/time_zone.hpp"

#include "tenzir/detail/assert.hpp"

#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include <fmt/format.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace tenzir {

struct TimeZone::Impl {
  std::string name;
  absl::TimeZone zone;
};

namespace {

/// The civil time that `absl::CivilSecond` arithmetic counts from, matching
/// the epoch of `local_time`.
constexpr auto civil_epoch = absl::CivilSecond{1970, 1, 1, 0, 0, 0};

auto to_absl(time t) -> absl::Time {
  return absl::FromUnixNanos(t.time_since_epoch().count());
}

auto from_absl(absl::Time t) -> time {
  return time{std::chrono::nanoseconds{absl::ToUnixNanos(t)}};
}

/// Extracts the IANA name from a path into a zoneinfo directory, such as
/// `/usr/share/zoneinfo/Europe/Berlin`.
auto zone_name_from_path(std::string_view path) -> Option<std::string_view> {
  constexpr auto marker = std::string_view{"zoneinfo/"};
  auto const pos = path.rfind(marker);
  if (pos == std::string_view::npos) {
    return None{};
  }
  return path.substr(pos + marker.size());
}

/// Whether `name` has the shape of an IANA zone name, such as
/// `America/Argentina/Buenos_Aires`. Zone names come from event data, so this
/// also keeps them from addressing files outside the zoneinfo directory.
auto is_zone_name(std::string_view name) -> bool {
  if (name.empty() or name.front() == '/' or name.back() == '/') {
    return false;
  }
  auto component_start = size_t{0};
  for (auto i = size_t{0}; i <= name.size(); ++i) {
    if (i == name.size() or name[i] == '/') {
      auto const component = name.substr(component_start, i - component_start);
      if (component.empty() or component == "." or component == "..") {
        return false;
      }
      component_start = i + 1;
      continue;
    }
    auto const c = name[i];
    auto const valid = (c >= 'A' and c <= 'Z') or (c >= 'a' and c <= 'z')
                       or (c >= '0' and c <= '9') or c == '_' or c == '-'
                       or c == '+';
    if (not valid) {
      return false;
    }
  }
  return true;
}

/// The directory that holds the zone files. `TZDIR` takes precedence. Otherwise,
/// this searches the same locations as the date library that Arrow vendors:
/// the database that the build configured, such as the tzdata of a Nix build,
/// then the system locations, including the one of Buildroot with uClibc.
auto zoneinfo_directory() -> std::string {
  if (auto const* dir = std::getenv("TZDIR"); dir != nullptr and *dir != '\0') {
    return dir;
  }
  constexpr auto system = std::string_view{"/usr/share/zoneinfo"};
  auto const candidates = std::array{
#ifdef TENZIR_ZONEINFO_DIR
    std::string_view{TENZIR_ZONEINFO_DIR},
#endif
#ifndef __APPLE__
    std::string_view{"/usr/share/zoneinfo/uclibc"},
#endif
    system,
#ifndef __APPLE__
    std::string_view{"/etc/zoneinfo"},
#endif
  };
  for (auto candidate : candidates) {
    auto ec = std::error_code{};
    if (std::filesystem::is_directory(candidate, ec)) {
      return std::string{candidate};
    }
  }
  return std::string{system};
}

} // namespace

auto TimeZone::locate(std::string_view name) -> Option<TimeZone const&> {
  // Abseil also accepts paths, `localtime`, and fixed offsets. We only accept
  // zone names, and resolve the host zone through `current`.
  if (not is_zone_name(name) or name == "localtime") {
    return None{};
  }
  // Zones are interned forever, like the entries of `std::chrono::tzdb`. Only
  // known zones enter the registry, which bounds its size by the database.
  static auto mutex = std::mutex{};
  static auto registry = std::map<std::string, Box<TimeZone>, std::less<>>{};
  auto lock = std::scoped_lock{mutex};
  if (auto it = registry.find(name); it != registry.end()) {
    return *it->second;
  }
  // Abseil loads absolute paths as they are, which lets us choose the
  // directory. UTC is built in and needs no file.
  auto const path = name == "UTC"
                      ? std::string{name}
                      : fmt::format("{}/{}", zoneinfo_directory(), name);
  auto zone = absl::TimeZone{};
  if (not absl::LoadTimeZone(path, &zone)) {
    return None{};
  }
  auto impl = Box<Impl>{Impl{std::string{name}, zone}};
  // The constructor is private, which rules out `std::make_unique`.
  auto entry = Box<TimeZone>::from_non_null(
    std::unique_ptr<TimeZone>{new TimeZone{std::move(impl)}});
  auto [it, inserted] = registry.emplace(std::string{name}, std::move(entry));
  TENZIR_ASSERT(inserted);
  return *it->second;
}

auto TimeZone::utc() -> TimeZone const& {
  // UTC is built into Abseil and resolves even without a database.
  static auto const& result = *locate("UTC");
  return result;
}

auto TimeZone::current() -> Option<TimeZone const&> {
  // A `TZ` of the form `:Europe/Berlin` or a path into a zoneinfo directory
  // names the zone like the C library reads it.
  if (auto const* tz = std::getenv("TZ"); tz != nullptr) {
    // An empty `TZ` is set, and means UTC, unlike an unset one.
    if (*tz == '\0') {
      return TimeZone::utc();
    }
    auto name = std::string_view{tz};
    if (name.starts_with(':')) {
      name.remove_prefix(1);
    }
    if (auto path_name = zone_name_from_path(name)) {
      name = *path_name;
    }
    return locate(name);
  }
  // Most systems link `/etc/localtime` into their zoneinfo directory.
  auto ec = std::error_code{};
  auto const target
    = std::filesystem::read_symlink("/etc/localtime", ec).string();
  if (not ec) {
    if (auto name = zone_name_from_path(target)) {
      return locate(*name);
    }
  }
  // Debian-based systems may copy the zone file instead and record its name.
  auto file = std::ifstream{"/etc/timezone"};
  auto name = std::string{};
  if (file and std::getline(file, name)) {
    return locate(name);
  }
  return None{};
}

TimeZone::TimeZone(Box<Impl> impl) : impl_{std::move(impl)} {
}

TimeZone::~TimeZone() = default;

auto TimeZone::to_local(time t) const -> local_time {
  return local_time{t.time_since_epoch() + offset(t)};
}

auto TimeZone::to_utc(local_time t) const -> time {
  auto const seconds = std::chrono::floor<std::chrono::seconds>(t);
  auto const subseconds = t - seconds;
  auto const civil = civil_epoch + seconds.time_since_epoch().count();
  // `pre` applies the offset in effect before a transition, which yields the
  // earlier instant for repeated and the later one for skipped civil times.
  return from_absl(impl_->zone.At(civil).pre) + subseconds;
}

auto TimeZone::offset(time t) const -> std::chrono::seconds {
  return std::chrono::seconds{impl_->zone.At(to_absl(t)).offset};
}

auto TimeZone::next_transition(time t) const -> Option<time> {
  auto transition = absl::TimeZone::CivilTransition{};
  if (not impl_->zone.NextTransition(to_absl(t), &transition)) {
    return None{};
  }
  // The civil time after a transition is unique when clocks spring forward and
  // repeated when they fall back. Either way, `trans` is the transition.
  return from_absl(impl_->zone.At(transition.to).trans);
}

auto TimeZone::name() const -> std::string_view {
  return impl_->name;
}

} // namespace tenzir
