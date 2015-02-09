#ifndef VAST_UTIL_ACCUMULATOR_H
#define VAST_UTIL_ACCUMULATOR_H

#include "vast/config.h"

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
#endif

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

  bacc::accumulator_set<T, features> accumulator_;
};

} // namespace util
} // namespace vast

#endif
