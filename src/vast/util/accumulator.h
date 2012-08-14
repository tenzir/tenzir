#ifndef VAST_UTIL_ACCUMULATOR_H
#define VAST_UTIL_ACCUMULATOR_H

// Boost Accumulators spits out quite a few warnings, which we'll disable here.
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
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

namespace vast {
namespace util {

namespace bacc = boost::accumulators;

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

private:
  typedef bacc::features<
      bacc::tag::count
   ,  bacc::tag::sum
   ,  bacc::tag::min
   ,  bacc::tag::max
   ,  bacc::tag::mean
   ,  bacc::tag::median
   ,  bacc::tag::variance
  > features;

  typedef bacc::accumulator_set<T, features> accumulator_type;

  accumulator_type accumulator_;
};

/// Accumulates values at a given resolution.
template <typename T>
class temporal_accumulator : public accumulator<T>
{
public:
  typedef std::chrono::system_clock clock;

  /// Constructs a temporal accumulator with a specific resolution.
  temporal_accumulator(clock::duration resolution)
    : resolution_(resolution)
  {
  }

  bool timed_add(T x)
  {
    current_value_ += x;
    auto now = clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        now - last_time_);

    if (elapsed < resolution_)
      return false;

    last_value_ = current_value_ * static_cast<T>(1000000) / elapsed.count();
    this->add(last_value_);

    last_time_ = now;
    current_value_ = 0;
    
    return true;
  }

  T current() const
  {
    return current_value_;
  }

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

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#endif
