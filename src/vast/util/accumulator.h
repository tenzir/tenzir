#ifndef VAST_UTIL_ACCUMULATOR_H
#define VAST_UTIL_ACCUMULATOR_H

#include "vast/config.h"

// Boost Accumulators spits out quite a few warnings, which we'll disable here.
#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
#endif

#include <chrono>
#include <boost/accumulators/framework/accumulator_set.hpp>
#include <boost/accumulators/framework/extractor.hpp>
#include <boost/accumulators/framework/features.hpp>
#include <boost/accumulators/statistics/count.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/sum.hpp>
#include <boost/accumulators/statistics/variance.hpp>

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

namespace vast {
namespace util {

namespace bacc = boost::accumulators;

/// A numerical accumulator for computing various online statistical
/// estimators with constant space, including *sum*, *min*, *max*, *mean*,
/// *median*, and *variance*.
template <typename T = double>
class accumulator
{
  static_assert(std::is_arithmetic<T>::value,
                "accumulator needs arithmetic type");
public:
  accumulator() = default;

  void add(T x)
  {
    accumulator_(x);
  }

  T count() const
  {
    return bacc::count(accumulator_);
  }

  T sum() const
  {
    return bacc::sum(accumulator_);
  }

  T min() const
  {
    return bacc::min(accumulator_);
  }

  T max() const
  {
    return bacc::max(accumulator_);
  }

  T mean() const
  {
    return bacc::mean(accumulator_);
  }

  T median() const
  {
    return bacc::median(accumulator_);
  }

  T variance() const
  {
    return bacc::variance(accumulator_);
  }

  auto sd() const
    -> decltype(std::sqrt(std::declval<accumulator>().variance()))
  {
    return std::sqrt(variance());
  }

private:
  using features = bacc::features<
    bacc::tag::count,
    bacc::tag::sum,
    bacc::tag::min,
    bacc::tag::max,
    bacc::tag::mean,
    bacc::tag::median,
    bacc::tag::variance
  >;

  using accumulator_type = bacc::accumulator_set<T, features>;

  accumulator_type accumulator_;
};

/// Accumulates values at a given resolution to allow for computation of rates.
/// The interface offers the functionality of an incrementable counter whose
/// value gets committed after a configured time resolution.
template <typename T>
class rate_accumulator : public accumulator<T>
{
public:
  using clock = std::chrono::system_clock;

  /// Constructs a temporal accumulator with a specific resolution.
  /// @param resolution The desired resolution.
  rate_accumulator(clock::duration resolution)
    : resolution_{resolution}
  {
  }

  /// Increments the internal counter by a given value.
  ///
  /// @param x The value to increment the internal counter.
  ///
  /// @returns `false` if *x* has been added to the current counter value
  /// within the configured resolution, and `true` if the addition of *x*
  /// committed the current counter value to the underlying accumuator.
  ///
  /// @post `current() == 0` upon returning `true`.
  bool increment(T x = 1)
  {
    current_value_ += x;
    auto now = clock::now();
    auto elapsed =
      std::chrono::duration_cast<std::chrono::microseconds>(now - last_time_);

    if (elapsed < resolution_)
      return false;

    last_value_ = current_value_ * static_cast<T>(1000000) / elapsed.count();
    last_time_ = now;
    current_value_ = 0;

    this->add(last_value_);

    return this->count() > 1;
  }

  /// Retrieves the current counter value.
  T current() const
  {
    return current_value_;
  }

  /// Retrieves the last value committed to the underlying accumulator.
  T last() const
  {
    return last_value_;
  }

private:
  clock::time_point last_time_;
  clock::duration resolution_;
  T last_value_ = 0;
  T current_value_ = 0;
};

} // namespace util
} // namespace vast

#endif
