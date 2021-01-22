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

  void restart() {
    start_ = stopwatch::now();
  }

  void stop(uint64_t events) {
    auto elapsed = stopwatch::now() - start_;
    m_ += {elapsed, events};
  }

private:
  stopwatch::time_point start_ = stopwatch::now();
  measurement& m_;
};

/// A collapsable benchmark mixin.
/// Defines all methods empty.
struct noop_benchmark_mixin {
  /// An iteration tracker that has noop implementation.
  struct iteration_tracker {
    constexpr void next_step() const noexcept {
    }
  };

  template <class... Args>
  constexpr void append_benchmark_metrics(Args&&...) const noexcept {
  }

  constexpr iteration_tracker make_iteration_tracker() const noexcept {
    return {};
  }
};

/// A real measuring  benchmark mixin based on `timer`.
/// Coniders the number of steps is equal to N.
template <int N>
class timer_benchmark_mixin {
public:
  void append_benchmark_metrics(std::vector<measurement>& measurements) {
    measurements.insert(measurements.end(), measurements_.begin(),
                        measurements_.end());
  }

  friend class iteration_tracker;
  class iteration_tracker {
  public:
    explicit iteration_tracker(timer_benchmark_mixin& totals) noexcept
      : totals_{totals} {
    }

    void next_step() noexcept {
      t.stop(1);
      totals_.measurements_[current_step_++] += m;
      m = {};
      t.restart();
    }

  private:
    int current_step_ = 0;
    measurement m{};
    timer t{m};
    timer_benchmark_mixin& totals_;
  };

  constexpr iteration_tracker make_iteration_tracker() noexcept {
    return iteration_tracker{*this};
  }

private:
  std::array<measurement, N> measurements_;
};

template <class Reader, class = void>
struct has_benchmark_metrics : std::false_type {};

template <class Reader>
struct has_benchmark_metrics<
  Reader, std::void_t<decltype(std::declval<Reader>().append_benchmark_metrics(
            std::declval<std::vector<measurement>&>()))>> : std::true_type {};

} // namespace vast::system
