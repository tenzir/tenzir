//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/option.hpp"
#include "tenzir/time.hpp"

#include <chrono>
#include <string_view>

namespace tenzir {

/// A wall-clock time that belongs to no particular time zone.
using local_time = std::chrono::local_time<duration>;

/// An IANA time zone, such as `Europe/Berlin`, backed by the time zone database
/// of the operating system. The `TZDIR` environment variable overrides the
/// database directory. Builds may configure a preferred directory with
/// `TENZIR_ZONEINFO_DIR`, which applies where it exists, such as the tzdata of
/// a Nix build.
///
/// Time zones are interned for the lifetime of the process, like the entries of
/// `std::chrono::tzdb`. References to them stay valid and are cheap to pass
/// around, so callers can look up a zone once and keep it.
class TimeZone {
public:
  /// Returns the zone with the IANA name `name`, or `None` if the database
  /// has no such zone. The host-dependent `localtime` is not a zone name.
  static auto locate(std::string_view name) -> Option<TimeZone const&>;

  /// Returns UTC, which needs no time zone database.
  static auto utc() -> TimeZone const&;

  /// Returns the time zone of the host, as set through the `TZ` environment
  /// variable or the system configuration, or `None` if it is unknown.
  static auto current() -> Option<TimeZone const&>;

  TimeZone(TimeZone const&) = delete;
  auto operator=(TimeZone const&) -> TimeZone& = delete;
  TimeZone(TimeZone&&) = delete;
  auto operator=(TimeZone&&) -> TimeZone& = delete;
  ~TimeZone();

  /// Returns the wall-clock time in this zone at the instant `t`.
  auto to_local(time t) const -> local_time;

  /// Returns the instant at which the wall-clock time in this zone reads `t`.
  ///
  /// When clocks fall back, a repeated wall-clock time resolves to the earlier
  /// of its two instants. When clocks spring forward, a skipped wall-clock time
  /// shifts forward by the length of the gap, so 02:30 in a one-hour gap at
  /// 02:00 becomes 03:30. Both follow from applying the UTC offset in effect
  /// before the transition.
  auto to_utc(local_time t) const -> time;

  /// Returns the UTC offset in effect at the instant `t`.
  auto offset(time t) const -> std::chrono::seconds;

  /// Returns the first instant after `t` at which the UTC offset changes, or
  /// `None` if it never changes again.
  auto next_transition(time t) const -> Option<time>;

  /// Returns the IANA name of the zone.
  auto name() const -> std::string_view;

private:
  struct Impl;

  explicit TimeZone(Box<Impl> impl);

  Box<Impl> impl_;
};

} // namespace tenzir
