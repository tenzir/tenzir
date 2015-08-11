#ifndef VAST_UTIL_RANDOM
#define VAST_UTIL_RANDOM

#include <random>
#include "vast/util/operators.h"

namespace vast {
namespace util {

/// Generates random numbers according to the [Pareto
/// distribution](http://en.wikipedia.org/wiki/Pareto_distribution).
template <typename RealType = double>
class pareto_distribution : equality_comparable<pareto_distribution<RealType>> {
public:
  using result_type = RealType;

  class param_type : equality_comparable<param_type> {
  public:
    using distribution_type = pareto_distribution;

    explicit param_type(result_type shape, result_type scale)
      : shape_{shape}, scale_{scale} {
    }

    result_type shape() const {
      return shape_;
    }

    result_type scale() const {
      return scale_;
    }

    friend bool operator==(param_type const& lhs, param_type const& rhs) {
      return lhs.shape_ == rhs.shape_ && lhs.scale_ == rhs.scale_;
    }

  private:
    result_type shape_;
    result_type scale_;
  };

  pareto_distribution(result_type shape, result_type scale)
    : params_{shape, scale} {
  }

  pareto_distribution(param_type parm) : params_{std::move(parm)} {
  }

  template <typename URNG>
  result_type operator()(URNG& g);

  template <typename URNG>
  result_type operator()(URNG& g, param_type const& parm);

  param_type param() const {
    return params_;
  }

  void param(param_type parm) {
    params_ = std::move(parm);
  }

  result_type shape() const {
    return params_.shape();
  }

  result_type scale() const {
    return params_.scale();
  }

  friend bool operator==(pareto_distribution const& lhs,
                         pareto_distribution const& rhs) {
    return lhs.params_ == rhs.params_;
  }

private:
  param_type params_;
};

template <typename R>
R pdf(pareto_distribution<R> const& dist, R x) {
  auto shape = dist.shape();
  auto scale = dist.scale();

  if (x < scale)
    return 0.0;

  return shape * std::pow(scale, shape) / std::pow(x, shape + 1);
}

template <typename R>
R cdf(pareto_distribution<R> const& dist, R x) {
  auto shape = dist.shape();
  auto scale = dist.scale();

  if (x <= scale)
    return 0;

  return R{1} - std::pow(scale / x, shape);
}

template <typename R>
R quantile(pareto_distribution<R> const& dist, R p) {
  auto shape = dist.shape();
  auto scale = dist.scale();

  if (p == 0)
    return scale;

  if (p == 1)
    return std::numeric_limits<R>::max();

  return scale / std::pow(1 - p, 1 / shape);
}

template <typename R>
template <typename URNG>
R pareto_distribution<R>::operator()(URNG& g) {
  return quantile(*this, std::uniform_real_distribution<result_type>{}(g));
}

template <typename R>
template <typename URNG>
R pareto_distribution<R>::operator()(
  URNG& g, typename pareto_distribution<R>::param_type const& parm) {
  return pareto_distribution<R>{parm}(g);
}

} // namespace util
} // namespace vast

#endif
