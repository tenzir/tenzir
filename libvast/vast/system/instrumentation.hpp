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

#include "vast/config.hpp"
#include "vast/detail/operators.hpp"
#include "vast/time.hpp"

#include <caf/meta/type_name.hpp>

#include <chrono>
#include <cmath>

namespace vast::system {

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
  double rate_per_sec() const noexcept {
    if (duration.count() > 0)
      return std::round(static_cast<double>(events)
                        * decltype(duration)::period::den / duration.count());
    else
      return std::numeric_limits<double>::max();
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
    auto elapsed = std::chrono::duration_cast<duration>(stop - start_);
    m_ += {elapsed, events};
  }

private:
  stopwatch::time_point start_ = stopwatch::now();
  measurement& m_;
};

} // namespace vast::system
