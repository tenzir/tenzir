//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/config.hpp"
#include "vast/detail/operators.hpp"
#include "vast/time.hpp"

#include <chrono>
#include <cmath>

namespace vast {

using stopwatch = std::chrono::steady_clock;

struct measurement : public detail::addable<measurement> {
  vast::duration duration = vast::duration::zero();
  uint64_t events = 0;

  measurement() = default;

  measurement(vast::duration d, uint64_t e) : duration{d}, events{e} {
    // nop
  }

  measurement& operator+=(const measurement& next) {
    duration += next.duration;
    events += next.events;
    return *this;
  }

  /// Returns the rate of events per second in the current measurement.
  [[nodiscard]] double rate_per_sec() const noexcept {
    if (duration.count() > 0)
      return std::round(static_cast<double>(events)
                        * decltype(duration)::period::den / duration.count());
    else
      return std::numeric_limits<double>::max();
  }
};

template <class Inspector>
auto inspect(Inspector& f, measurement& x) {
  return f.object(x)
    .pretty_name("measurement")
    .fields(f.field("duration", x.duration), f.field("events", x.events));
}

struct timer {
  explicit timer(measurement& m) : m_{m} {
    // nop
  }

  static timer start(measurement& m) {
    return timer{m};
  }

  void stop(uint64_t events) {
    auto elapsed = stopwatch::now() - start_;
    m_ += {elapsed, events};
  }

private:
  stopwatch::time_point start_ = stopwatch::now();
  measurement& m_;
};

} // namespace vast
