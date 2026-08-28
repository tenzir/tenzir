//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Adapted from Apache Arrow to fit Tenzir's coding style.

#include "tenzir/detail/tdigest.hpp"

#include "tenzir/detail/assert.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <numbers>
#include <queue>
#include <tuple>
#include <vector>

namespace tenzir::detail {

namespace {

/// Returns the relative position of `value` in [`left`, `right`] without
/// overflowing the interval width.
auto relative_position(double left, double right, double value) -> double {
  const auto width = right - left;
  if (std::isfinite(width)) {
    return (value - left) / width;
  }
  return (value / 2.0 - left / 2.0) / (right / 2.0 - left / 2.0);
}

/// Multiplies an interval width by a non-negative scale without overflowing
/// the width before applying the scale.
auto scale_interval(double left, double right, double scale) -> double {
  if (scale == 0.0) {
    return 0.0;
  }
  const auto width = right - left;
  if (std::isfinite(width)) {
    return width * scale;
  }
  return right * scale - left * scale;
}

/// Integrates the absolute value of the line from `y0` to `y1` over an
/// interval from `left` to `right`.
auto integrate_abs_line(double y0, double y1, double left, double right)
  -> double {
  if (std::signbit(y0) == std::signbit(y1) or y0 == 0.0 or y1 == 0.0) {
    return scale_interval(left, right, (std::abs(y0) + std::abs(y1)) / 2.0);
  }
  const auto a = std::abs(y0);
  const auto b = std::abs(y1);
  const auto high = std::max(a, b);
  const auto ratio = std::min(a, b) / high;
  const auto scale = high * (1.0 + ratio * ratio) / (2.0 * (1.0 + ratio));
  return scale_interval(left, right, scale);
}

// histogram bin
struct centroid {
  double mean;
  double weight; // # data points in this bin

  // merge with another centroid
  void merge(const centroid& other) {
    weight += other.weight;
    mean = std::lerp(mean, other.mean, other.weight / weight);
  }
};

// scale function K0: linear function, as baseline
struct scaler_k0 {
  explicit scaler_k0(uint32_t delta) : delta_norm(delta / 2.0) {
  }

  auto k(double q) const -> double {
    return delta_norm * q;
  }
  auto q(double k_val) const -> double {
    return k_val / delta_norm;
  }

  const double delta_norm;
};

// scale function K1
struct scaler_k1 {
  explicit scaler_k1(uint32_t delta)
    : delta_norm(delta / (2.0 * std::numbers::pi)) {
  }

  auto k(double q) const -> double {
    return delta_norm * std::asin(2 * q - 1);
  }
  auto q(double k_val) const -> double {
    return (std::sin(k_val / delta_norm) + 1) / 2;
  }

  const double delta_norm;
};

// implements t-digest merging algorithm
template <class T = scaler_k1>
class tdigest_merger : private T {
public:
  explicit tdigest_merger(uint32_t delta) : T(delta) {
    reset(0, nullptr);
  }

  auto reset(double total_weight, std::vector<centroid>* tdigest) -> void {
    total_weight_ = total_weight;
    tdigest_ = tdigest;
    if (tdigest_) {
      tdigest_->resize(0);
    }
    weight_so_far_ = 0;
    weight_limit_ = -1; // trigger first centroid merge
  }

  // merge one centroid from a sorted centroid stream
  auto add(const centroid& c) -> void {
    auto& td = *tdigest_;
    auto weight = weight_so_far_ + c.weight;
    if (weight <= weight_limit_) {
      td.back().merge(c);
    } else {
      auto quantile = weight_so_far_ / total_weight_;
      auto next_weight_limit = total_weight_ * this->q(this->k(quantile) + 1);
      // weight limit should be strictly increasing, until the last centroid
      if (next_weight_limit <= weight_limit_) {
        weight_limit_ = total_weight_;
      } else {
        weight_limit_ = next_weight_limit;
      }
      td.push_back(c); // should never exceed capacity and trigger reallocation
    }
    weight_so_far_ = weight;
  }

  // validate k-size of a tdigest
  auto validate(const std::vector<centroid>& tdigest, double total_weight) const
    -> Result<void, std::string> {
    auto q_prev = 0.0;
    auto k_prev = this->k(0);
    for (size_t i = 0; i < tdigest.size(); ++i) {
      auto q = q_prev + tdigest[i].weight / total_weight;
      auto k_val = this->k(q);
      if (tdigest[i].weight != 1 and (k_val - k_prev) > 1.001) {
        return Err{fmt::format("oversized centroid: {}", k_val - k_prev)};
      }
      k_prev = k_val;
      q_prev = q;
    }
    return {};
  }

private:
  double total_weight_;  // total weight of this tdigest
  double weight_so_far_; // accumulated weight till current bin
  double weight_limit_;  // max accumulated weight to move to next bin
  std::vector<centroid>* tdigest_;
};

} // namespace

class tdigest::tdigest_impl {
public:
  explicit tdigest_impl(uint32_t delta)
    : delta_(delta > 10 ? delta : 10), merger_(delta_) {
    tdigests_[0].reserve(delta_);
    tdigests_[1].reserve(delta_);
    reset();
  }

  auto reset() -> void {
    tdigests_[0].resize(0);
    tdigests_[1].resize(0);
    current_ = 0;
    total_weight_ = 0;
    min_ = std::numeric_limits<double>::max();
    max_ = std::numeric_limits<double>::lowest();
    merger_.reset(0, nullptr);
  }

  auto validate() const -> Result<void, std::string> {
    // check weight, centroid order
    auto total_weight = 0.0;
    auto prev_mean = std::numeric_limits<double>::lowest();
    for (const auto& c : tdigests_[current_]) {
      if (not std::isfinite(c.mean) or not std::isfinite(c.weight)) {
        return Err{"non-finite value found in tdigest"};
      }
      if (c.mean < prev_mean) {
        return Err{"centroid mean decreases"};
      }
      if (c.weight < 1) {
        return Err{"invalid centroid weight"};
      }
      prev_mean = c.mean;
      total_weight += c.weight;
    }
    if (total_weight != total_weight_) {
      return Err{"tdigest total weight mismatch"};
    }
    // check if buffer expanded
    if (tdigests_[0].capacity() > delta_ or tdigests_[1].capacity() > delta_) {
      return Err{"oversized tdigest buffer"};
    }
    // check k-size
    return merger_.validate(tdigests_[current_], total_weight_);
  }

  auto dump() const -> void {
    const auto& td = tdigests_[current_];
    for (size_t i = 0; i < td.size(); ++i) {
      std::cerr << i << ": mean = " << td[i].mean
                << ", weight = " << td[i].weight << std::endl;
    }
    std::cerr << "min = " << min_ << ", max = " << max_ << std::endl;
  }

  // merge with other tdigests
  auto merge(const std::vector<const tdigest_impl*>& tdigest_impls) -> void {
    // current and end iterator
    using centroid_iter = std::vector<centroid>::const_iterator;
    using centroid_iter_pair = std::pair<centroid_iter, centroid_iter>;
    // use a min-heap to find next minimal centroid from all tdigests
    auto centroid_gt
      = [](const centroid_iter_pair& lhs, const centroid_iter_pair& rhs) {
          return lhs.first->mean > rhs.first->mean;
        };
    using centroid_queue
      = std::priority_queue<centroid_iter_pair, std::vector<centroid_iter_pair>,
                            decltype(centroid_gt)>;

    // trivial dynamic memory allocated at runtime
    std::vector<centroid_iter_pair> queue_buffer;
    queue_buffer.reserve(tdigest_impls.size() + 1);
    centroid_queue queue(std::move(centroid_gt), std::move(queue_buffer));
    const auto& this_tdigest = tdigests_[current_];
    if (this_tdigest.size() > 0) {
      queue.emplace(this_tdigest.cbegin(), this_tdigest.cend());
    }
    for (const tdigest_impl* td : tdigest_impls) {
      const auto& other_tdigest = td->tdigests_[td->current_];
      if (other_tdigest.size() > 0) {
        queue.emplace(other_tdigest.cbegin(), other_tdigest.cend());
        total_weight_ += td->total_weight_;
        min_ = std::min(min_, td->min_);
        max_ = std::max(max_, td->max_);
      }
    }
    merger_.reset(total_weight_, &tdigests_[1 - current_]);
    centroid_iter current_iter, end_iter;
    // do k-way merge till one buffer left
    while (queue.size() > 1) {
      std::tie(current_iter, end_iter) = queue.top();
      merger_.add(*current_iter);
      queue.pop();
      if (++current_iter != end_iter) {
        queue.emplace(current_iter, end_iter);
      }
    }
    // merge last buffer
    if (not queue.empty()) {
      std::tie(current_iter, end_iter) = queue.top();
      while (current_iter != end_iter) {
        merger_.add(*current_iter++);
      }
    }
    merger_.reset(0, nullptr);
    current_ = 1 - current_;
  }

  // merge input data with current tdigest
  auto merge_input(std::vector<double>& input) -> void {
    total_weight_ += input.size();
    std::sort(input.begin(), input.end());
    min_ = std::min(min_, input.front());
    max_ = std::max(max_, input.back());
    // pick next minimal centroid from input and tdigest, feed to merger
    merger_.reset(total_weight_, &tdigests_[1 - current_]);
    const auto& td = tdigests_[current_];
    auto tdigest_index = uint32_t{0};
    auto input_index = uint32_t{0};
    while (tdigest_index < td.size() and input_index < input.size()) {
      if (td[tdigest_index].mean < input[input_index]) {
        merger_.add(td[tdigest_index++]);
      } else {
        merger_.add(centroid{input[input_index++], 1});
      }
    }
    while (tdigest_index < td.size()) {
      merger_.add(td[tdigest_index++]);
    }
    while (input_index < input.size()) {
      merger_.add(centroid{input[input_index++], 1});
    }
    merger_.reset(0, nullptr);
    input.resize(0);
    current_ = 1 - current_;
  }

  auto quantile(double q) const -> double {
    const auto& td = tdigests_[current_];
    if (q < 0 or q > 1 or td.size() == 0) {
      return NAN;
    }

    auto index = q * total_weight_;
    if (index <= 1) {
      return min_;
    } else if (index >= total_weight_ - 1) {
      return max_;
    }

    // find centroid contains the index
    auto ci = uint32_t{0};
    auto weight_sum = 0.0;
    for (; ci < td.size(); ++ci) {
      weight_sum += td[ci].weight;
      if (index <= weight_sum) {
        break;
      }
    }
    TENZIR_ASSERT(ci < td.size());
    // deviation of index from the centroid center
    auto diff = index + td[ci].weight / 2 - weight_sum;
    // index happen to be in a unit weight centroid
    if (td[ci].weight == 1 and std::abs(diff) < 0.5) {
      return td[ci].mean;
    }
    // find adjacent centroids for interpolation
    auto ci_left = ci;
    auto ci_right = ci;
    if (diff > 0) {
      if (ci_right == td.size() - 1) {
        // index larger than center of last bin
        TENZIR_ASSERT(weight_sum == total_weight_);
        auto c = &td[ci_right];
        TENZIR_ASSERT(c->weight >= 2);
        return std::lerp(c->mean, max_, diff / (c->weight / 2));
      }
      ++ci_right;
    } else {
      if (ci_left == 0) {
        // index smaller than center of first bin
        auto c = &td[0];
        TENZIR_ASSERT(c->weight >= 2);
        return std::lerp(min_, c->mean, index / (c->weight / 2));
      }
      --ci_left;
      diff += td[ci_left].weight / 2 + td[ci_right].weight / 2;
    }
    // interpolate from adjacent centroids
    diff /= (td[ci_left].weight / 2 + td[ci_right].weight / 2);
    return std::lerp(td[ci_left].mean, td[ci_right].mean, diff);
  }

  auto cdf(double x) const -> double {
    const auto& td = tdigests_[current_];
    if (not std::isfinite(x) or td.empty()) {
      return NAN;
    }
    if (x < min_) {
      return 0.0;
    }
    if (x > max_) {
      return 1.0;
    }
    if (td.size() == 1) {
      const auto mean = td.front().mean;
      if (min_ == max_) {
        return 0.5;
      }
      if (x < mean) {
        return mean == min_ ? 0.0 : 0.5 * relative_position(min_, mean, x);
      }
      if (x > mean) {
        return mean == max_ ? 1.0
                            : 0.5 + 0.5 * relative_position(mean, max_, x);
      }
      return 0.5;
    }
    const auto& first = td.front();
    if (x < first.mean) {
      if (first.mean == min_) {
        return 0.0;
      }
      if (x == min_) {
        return 0.5 / total_weight_;
      }
      return (1.0
              + relative_position(min_, first.mean, x)
                  * (first.weight / 2.0 - 1.0))
             / total_weight_;
    }
    const auto& last = td.back();
    if (x > last.mean) {
      if (last.mean == max_) {
        return 1.0;
      }
      if (x == max_) {
        return 1.0 - 0.5 / total_weight_;
      }
      const auto remaining = (1.0
                              + (1.0 - relative_position(last.mean, max_, x))
                                  * (last.weight / 2.0 - 1.0))
                             / total_weight_;
      return 1.0 - remaining;
    }
    auto weight_so_far = 0.0;
    for (auto i = size_t{0}; i + 1 < td.size(); ++i) {
      if (td[i].mean == x) {
        auto equal_weight = 0.0;
        while (i < td.size() and td[i].mean == x) {
          equal_weight += td[i].weight;
          ++i;
        }
        return (weight_so_far + equal_weight / 2.0) / total_weight_;
      }
      if (td[i].mean < x and x < td[i + 1].mean) {
        if (td[i].weight == 1.0 and td[i + 1].weight == 1.0) {
          return (weight_so_far + 1.0) / total_weight_;
        }
        auto left_excluded = 0.0;
        auto right_excluded = 0.0;
        if (td[i].weight == 1.0) {
          left_excluded = 0.5;
        } else if (td[i + 1].weight == 1.0) {
          right_excluded = 0.5;
        }
        const auto span_weight = (td[i].weight + td[i + 1].weight) / 2.0;
        const auto interpolated_weight
          = span_weight - left_excluded - right_excluded;
        const auto base = weight_so_far + td[i].weight / 2.0 + left_excluded;
        return (base
                + interpolated_weight
                    * relative_position(td[i].mean, td[i + 1].mean, x))
               / total_weight_;
      }
      weight_so_far += td[i].weight;
    }
    TENZIR_ASSERT(x == td.back().mean);
    return 1.0 - td.back().weight / 2.0 / total_weight_;
  }

  /// Returns the CDF limits immediately before and after `x`.
  auto cdf_limits(double x) const -> std::pair<double, double> {
    const auto& td = tdigests_[current_];
    if (not std::isfinite(x) or td.empty()) {
      return {NAN, NAN};
    }
    if (x < min_) {
      return {0.0, 0.0};
    }
    if (x > max_) {
      return {1.0, 1.0};
    }
    if (td.size() == 1) {
      const auto mean = td.front().mean;
      if (min_ == max_) {
        return {0.0, 1.0};
      }
      if (x == mean) {
        return {mean == min_ ? 0.0 : 0.5, mean == max_ ? 1.0 : 0.5};
      }
      const auto value = cdf(x);
      return {value, value};
    }
    auto weight_before = 0.0;
    auto first_equal = size_t{0};
    while (first_equal < td.size() and td[first_equal].mean < x) {
      weight_before += td[first_equal].weight;
      ++first_equal;
    }
    if (first_equal == td.size() or td[first_equal].mean != x) {
      if (x == min_) {
        return {0.0, 1.0 / total_weight_};
      }
      if (x == max_) {
        return {1.0 - 1.0 / total_weight_, 1.0};
      }
      const auto value = cdf(x);
      return {value, value};
    }
    auto last_equal = first_equal;
    auto weight_before_last = weight_before;
    while (last_equal + 1 < td.size() and td[last_equal + 1].mean == x) {
      weight_before_last += td[last_equal].weight;
      ++last_equal;
    }
    const auto left
      = x == min_
          ? 0.0
          : (weight_before
             + (td[first_equal].weight == 1.0 ? 0.0
                                              : td[first_equal].weight / 2.0))
              / total_weight_;
    const auto right
      = x == max_
          ? 1.0
          : (weight_before_last
             + (td[last_equal].weight == 1.0 ? 1.0
                                             : td[last_equal].weight / 2.0))
              / total_weight_;
    return {left, right};
  }

  auto breakpoints() const -> std::vector<double> {
    auto result = std::vector<double>{};
    result.reserve(tdigests_[current_].size() + 2);
    result.push_back(min_);
    for (const auto& c : tdigests_[current_]) {
      result.push_back(c.mean);
    }
    result.push_back(max_);
    std::ranges::sort(result);
    const auto [first, last] = std::ranges::unique(result);
    result.erase(first, last);
    return result;
  }

  auto mean() const -> double {
    auto sum = 0.0;
    for (const auto& c : tdigests_[current_]) {
      sum += c.mean * c.weight;
    }
    return total_weight_ == 0 ? NAN : sum / total_weight_;
  }

  auto total_weight() const -> double {
    return total_weight_;
  }

  auto centroids() const -> const std::vector<centroid>& {
    return tdigests_[current_];
  }

  auto min_value() const -> double {
    return min_;
  }

  auto max_value() const -> double {
    return max_;
  }

  // rebuild the digest from merged centroids; returns false and resets the
  // digest if the centroids do not form a valid tdigest
  auto restore(std::vector<centroid> centroids, double min, double max)
    -> bool {
    reset();
    if (centroids.empty()) {
      return true;
    }
    if (centroids.size() > delta_ or not std::isfinite(min)
        or not std::isfinite(max) or min > centroids.front().mean
        or centroids.back().mean > max
        or (centroids.front().weight == 1 and min != centroids.front().mean)
        or (centroids.back().weight == 1 and max != centroids.back().mean)) {
      return false;
    }
    auto total_weight = 0.0;
    auto prev_mean = std::numeric_limits<double>::lowest();
    for (const auto& c : centroids) {
      if (not std::isfinite(c.mean) or not std::isfinite(c.weight)
          or c.weight < 1 or c.mean < prev_mean) {
        return false;
      }
      prev_mean = c.mean;
      total_weight += c.weight;
      if (not std::isfinite(total_weight)) {
        return false;
      }
    }
    tdigests_[current_] = std::move(centroids);
    total_weight_ = total_weight;
    min_ = min;
    max_ = max;
    return true;
  }

private:
  // must be declared before merger_, see constructor initialization list
  const uint32_t delta_;

  tdigest_merger<> merger_;
  double total_weight_;
  double min_, max_;
  // ping-pong buffer holds two tdigests, size = 2 * delta * sizeof(centroid)
  std::vector<centroid> tdigests_[2];
  // index of active tdigest buffer, 0 or 1
  int current_;
};

tdigest::tdigest(uint32_t delta, uint32_t buffer_size)
  : impl_{std::in_place, delta} {
  input_.reserve(buffer_size);
  reset();
}

tdigest::~tdigest() = default;
tdigest::tdigest(const tdigest&) = default;
tdigest& tdigest::operator=(const tdigest&) = default;
tdigest::tdigest(tdigest&&) = default;
tdigest& tdigest::operator=(tdigest&&) = default;

auto tdigest::reset() -> void {
  input_.resize(0);
  impl_->reset();
}

auto tdigest::validate() const -> Result<void, std::string> {
  merge_input();
  return impl_->validate();
}

auto tdigest::dump() const -> void {
  merge_input();
  impl_->dump();
}

auto tdigest::merge(const std::vector<tdigest>& others) -> void {
  merge_input();
  auto other_impls = std::vector<const tdigest_impl*>{};
  other_impls.reserve(others.size());
  for (auto& other : others) {
    other.merge_input();
    other_impls.push_back(&*other.impl_);
  }
  impl_->merge(other_impls);
}

auto tdigest::merge(const tdigest& other) -> void {
  merge_input();
  other.merge_input();
  impl_->merge({&*other.impl_});
}

auto tdigest::quantile(double q) const -> double {
  merge_input();
  return impl_->quantile(q);
}

auto tdigest::cdf(double x) const -> double {
  merge_input();
  return impl_->cdf(x);
}

auto tdigest::ks_distance(const tdigest& other) const -> double {
  merge_input();
  other.merge_input();
  if (impl_->total_weight() == 0 or other.impl_->total_weight() == 0) {
    return NAN;
  }
  auto points = impl_->breakpoints();
  auto other_points = other.impl_->breakpoints();
  points.insert(points.end(), other_points.begin(), other_points.end());
  std::ranges::sort(points);
  const auto [first, last] = std::ranges::unique(points);
  points.erase(first, last);
  auto result = 0.0;
  const auto check_at = [&](double x) {
    result = std::max(result, std::abs(impl_->cdf(x) - other.impl_->cdf(x)));
    const auto limits = impl_->cdf_limits(x);
    const auto other_limits = other.impl_->cdf_limits(x);
    result = std::max(result, std::abs(limits.first - other_limits.first));
    result = std::max(result, std::abs(limits.second - other_limits.second));
  };
  for (const auto x : points) {
    check_at(x);
  }
  return std::min(result, 1.0);
}

auto tdigest::wasserstein_distance(const tdigest& other) const -> double {
  merge_input();
  other.merge_input();
  if (impl_->total_weight() == 0 or other.impl_->total_weight() == 0) {
    return NAN;
  }
  auto points = impl_->breakpoints();
  auto other_points = other.impl_->breakpoints();
  points.insert(points.end(), other_points.begin(), other_points.end());
  std::ranges::sort(points);
  const auto [first, last] = std::ranges::unique(points);
  points.erase(first, last);
  auto result = 0.0;
  for (auto i = size_t{0}; i + 1 < points.size(); ++i) {
    const auto a = points[i];
    const auto b = points[i + 1];
    if (a == b) {
      continue;
    }
    const auto limits_at_a = impl_->cdf_limits(a);
    const auto other_limits_at_a = other.impl_->cdf_limits(a);
    const auto limits_at_b = impl_->cdf_limits(b);
    const auto other_limits_at_b = other.impl_->cdf_limits(b);
    const auto d0 = limits_at_a.second - other_limits_at_a.second;
    const auto d1 = limits_at_b.first - other_limits_at_b.first;
    result += integrate_abs_line(d0, d1, a, b);
  }
  return result;
}

auto tdigest::mean() const -> double {
  merge_input();
  return impl_->mean();
}

auto tdigest::is_empty() const -> bool {
  return input_.size() == 0 and impl_->total_weight() == 0;
}

auto tdigest::save() const -> tdigest_state {
  merge_input();
  auto result = tdigest_state{};
  const auto& centroids = impl_->centroids();
  result.means.reserve(centroids.size());
  result.weights.reserve(centroids.size());
  for (const auto& c : centroids) {
    result.means.push_back(c.mean);
    result.weights.push_back(c.weight);
  }
  result.min = impl_->min_value();
  result.max = impl_->max_value();
  return result;
}

auto tdigest::restore(const tdigest_state& state) -> bool {
  input_.resize(0);
  if (state.means.size() != state.weights.size()) {
    impl_->reset();
    return false;
  }
  auto centroids = std::vector<centroid>{};
  centroids.reserve(state.means.size());
  for (size_t i = 0; i < state.means.size(); ++i) {
    centroids.push_back(centroid{state.means[i], state.weights[i]});
  }
  return impl_->restore(std::move(centroids), state.min, state.max);
}

auto tdigest::merge_input() const -> void {
  if (input_.size() > 0) {
    impl_->merge_input(input_); // will mutate input_
  }
}

} // namespace tenzir::detail
