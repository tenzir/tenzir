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

#pragma once

#include <caf/meta/type_name.hpp>

#include "vast/detail/operators.hpp"
#include "vast/time.hpp"

#include <atomic>
#include <chrono>

namespace vast::system {

using stopwatch = std::chrono::steady_clock;

struct measurement : public detail::addable<measurement> {
  using timespan = vast::timespan;
  timespan duration = timespan::zero();
  uint64_t events = 0;

  measurement() = default;

  measurement(timespan d, uint64_t e) : duration{d}, events{e} {
    // nop
  }

  measurement& operator+=(const measurement& next) {
    duration += next.duration;
    events += next.events;
    return *this;
  }
};

template <class Inspector>
auto inspect(Inspector& f, measurement& x) {
  return f(caf::meta::type_name("measurement"), x.duration, x.events);
}

struct timer {
  explicit timer(measurement& m) : m_{m} {
    // nop
  }

  static timer start(measurement& m) {
    return timer{m};
  }

  void stop(uint64_t events) {
    auto stop = stopwatch::now();
    auto elapsed = std::chrono::duration_cast<measurement::timespan>(stop - start_);
    m_ += {elapsed, events};
  }

private:
  stopwatch::time_point start_ = stopwatch::now();
  measurement& m_;
};

// Atomic variants

using atomic_measurement = std::atomic<measurement>;

struct atomic_timer {
  explicit atomic_timer(atomic_measurement& m) : m_{m} {
    // nop
  }

  static atomic_timer start(atomic_measurement& m) {
    return atomic_timer{m};
  }

  void stop(uint64_t events) {
    auto stop = stopwatch::now();
    auto elapsed = std::chrono::duration_cast<measurement::timespan>(stop - start_);
    auto tmp = m_.load();
    tmp += measurement{elapsed, events};
    m_.exchange(tmp);
  }

private:
  stopwatch::time_point start_ = stopwatch::now();
  atomic_measurement& m_;
};

} // namespace vast::system
