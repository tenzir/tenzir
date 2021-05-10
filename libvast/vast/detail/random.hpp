//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/operators.hpp"

#include <random>

namespace vast::detail {

/// Generates random numbers according to the [Pareto
/// distribution](http://en.wikipedia.org/wiki/Pareto_distribution).
template <class RealType = double>
class pareto_distribution : equality_comparable<pareto_distribution<RealType>> {
public:
  using result_type = RealType;

  class param_type : equality_comparable<param_type> {
  public:
    using distribution_type = pareto_distribution;

    explicit param_type(result_type shape, result_type scale)
      : shape_{shape}, scale_{scale} {
    }

    [[nodiscard]] result_type shape() const {
      return shape_;
    }

    [[nodiscard]] result_type scale() const {
      return scale_;
    }

    friend bool operator==(const param_type& lhs, const param_type& rhs) {
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

  template <class URNG>
  result_type operator()(URNG& g);

  template <class URNG>
  result_type operator()(URNG& g, const param_type& parm);

  [[nodiscard]] param_type param() const {
    return params_;
  }

  void param(param_type parm) {
    params_ = std::move(parm);
  }

  [[nodiscard]] result_type shape() const {
    return params_.shape();
  }

  [[nodiscard]] result_type scale() const {
    return params_.scale();
  }

  friend bool operator==(const pareto_distribution& lhs,
                         const pareto_distribution& rhs) {
    return lhs.params_ == rhs.params_;
  }

private:
  param_type params_;
};

template <class R>
R pdf(const pareto_distribution<R>& dist, R x) {
  auto shape = dist.shape();
  auto scale = dist.scale();
  if (x < scale)
    return 0.0;
  return shape * std::pow(scale, shape) / std::pow(x, shape + 1);
}

template <class R>
R cdf(const pareto_distribution<R>& dist, R x) {
  auto shape = dist.shape();
  auto scale = dist.scale();
  if (x <= scale)
    return 0;
  return R{1} - std::pow(scale / x, shape);
}

template <class R>
R quantile(const pareto_distribution<R>& dist, R p) {
  auto shape = dist.shape();
  auto scale = dist.scale();
  if (p == 0)
    return scale;
  if (p == 1)
    return std::numeric_limits<R>::max();
  return scale / std::pow(1 - p, 1 / shape);
}

template <class R>
template <class URNG>
R pareto_distribution<R>::operator()(URNG& g) {
  return quantile(*this, std::uniform_real_distribution<result_type>{}(g));
}

template <class R>
template <class URNG>
R pareto_distribution<R>::operator()(
  URNG& g, const typename pareto_distribution<R>::param_type& parm) {
  return pareto_distribution<R>{parm}(g);
}

} // namespace vast::detail
