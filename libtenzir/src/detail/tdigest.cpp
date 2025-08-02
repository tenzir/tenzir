//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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

// a numerically stable lerp is unbelievably complex
// but we are *approximating* the quantile, so let's keep it simple
auto lerp(double a, double b, double t) -> double {
  return a + t * (b - a);
}

// histogram bin
struct centroid {
  double mean;
  double weight; // # data points in this bin

  // merge with another centroid
  void merge(const centroid& other) {
    weight += other.weight;
    mean += (other.mean - mean) * other.weight / weight;
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

  void reset(double total_weight, std::vector<centroid>* tdigest) {
    total_weight_ = total_weight;
    tdigest_ = tdigest;
    if (tdigest_) {
      tdigest_->resize(0);
    }
    weight_so_far_ = 0;
    weight_limit_ = -1; // trigger first centroid merge
  }

  // merge one centroid from a sorted centroid stream
  void add(const centroid& c) {
    auto& td = *tdigest_;
    const double weight = weight_so_far_ + c.weight;
    if (weight <= weight_limit_) {
      td.back().merge(c);
    } else {
      const double quantile = weight_so_far_ / total_weight_;
      const double next_weight_limit
        = total_weight_ * this->q(this->k(quantile) + 1);
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
    -> std::expected<void, std::string> {
    double q_prev = 0, k_prev = this->k(0);
    for (size_t i = 0; i < tdigest.size(); ++i) {
      const double q = q_prev + tdigest[i].weight / total_weight;
      const double k_val = this->k(q);
      if (tdigest[i].weight != 1 && (k_val - k_prev) > 1.001) {
        return std::unexpected(
          fmt::format("oversized centroid: {}", k_val - k_prev));
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

  void reset() {
    tdigests_[0].resize(0);
    tdigests_[1].resize(0);
    current_ = 0;
    total_weight_ = 0;
    min_ = std::numeric_limits<double>::max();
    max_ = std::numeric_limits<double>::lowest();
    merger_.reset(0, nullptr);
  }

  auto validate() const -> std::expected<void, std::string> {
    // check weight, centroid order
    double total_weight = 0, prev_mean = std::numeric_limits<double>::lowest();
    for (const auto& c : tdigests_[current_]) {
      if (std::isnan(c.mean) || std::isnan(c.weight)) {
        return std::unexpected("NAN found in tdigest");
      }
      if (c.mean < prev_mean) {
        return std::unexpected("centroid mean decreases");
      }
      if (c.weight < 1) {
        return std::unexpected("invalid centroid weight");
      }
      prev_mean = c.mean;
      total_weight += c.weight;
    }
    if (total_weight != total_weight_) {
      return std::unexpected("tdigest total weight mismatch");
    }
    // check if buffer expanded
    if (tdigests_[0].capacity() > delta_ || tdigests_[1].capacity() > delta_) {
      return std::unexpected("oversized tdigest buffer");
    }
    // check k-size
    return merger_.validate(tdigests_[current_], total_weight_);
  }

  void dump() const {
    const auto& td = tdigests_[current_];
    for (size_t i = 0; i < td.size(); ++i) {
      std::cerr << i << ": mean = " << td[i].mean
                << ", weight = " << td[i].weight << std::endl;
    }
    std::cerr << "min = " << min_ << ", max = " << max_ << std::endl;
  }

  // merge with other tdigests
  void merge(const std::vector<const tdigest_impl*>& tdigest_impls) {
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
    if (! queue.empty()) {
      std::tie(current_iter, end_iter) = queue.top();
      while (current_iter != end_iter) {
        merger_.add(*current_iter++);
      }
    }
    merger_.reset(0, nullptr);

    current_ = 1 - current_;
  }

  // merge input data with current tdigest
  void merge_input(std::vector<double>& input) {
    total_weight_ += input.size();

    std::sort(input.begin(), input.end());
    min_ = std::min(min_, input.front());
    max_ = std::max(max_, input.back());

    // pick next minimal centroid from input and tdigest, feed to merger
    merger_.reset(total_weight_, &tdigests_[1 - current_]);
    const auto& td = tdigests_[current_];
    uint32_t tdigest_index = 0, input_index = 0;
    while (tdigest_index < td.size() && input_index < input.size()) {
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

    if (q < 0 || q > 1 || td.size() == 0) {
      return NAN;
    }

    const double index = q * total_weight_;
    if (index <= 1) {
      return min_;
    } else if (index >= total_weight_ - 1) {
      return max_;
    }

    // find centroid contains the index
    uint32_t ci = 0;
    double weight_sum = 0;
    for (; ci < td.size(); ++ci) {
      weight_sum += td[ci].weight;
      if (index <= weight_sum) {
        break;
      }
    }
    TENZIR_ASSERT(ci < td.size());

    // deviation of index from the centroid center
    double diff = index + td[ci].weight / 2 - weight_sum;

    // index happen to be in a unit weight centroid
    if (td[ci].weight == 1 && std::abs(diff) < 0.5) {
      return td[ci].mean;
    }

    // find adjacent centroids for interpolation
    uint32_t ci_left = ci, ci_right = ci;
    if (diff > 0) {
      if (ci_right == td.size() - 1) {
        // index larger than center of last bin
        TENZIR_ASSERT(weight_sum == total_weight_);
        const centroid* c = &td[ci_right];
        TENZIR_ASSERT(c->weight >= 2);
        return lerp(c->mean, max_, diff / (c->weight / 2));
      }
      ++ci_right;
    } else {
      if (ci_left == 0) {
        // index smaller than center of first bin
        const centroid* c = &td[0];
        TENZIR_ASSERT(c->weight >= 2);
        return lerp(min_, c->mean, index / (c->weight / 2));
      }
      --ci_left;
      diff += td[ci_left].weight / 2 + td[ci_right].weight / 2;
    }

    // interpolate from adjacent centroids
    diff /= (td[ci_left].weight / 2 + td[ci_right].weight / 2);
    return lerp(td[ci_left].mean, td[ci_right].mean, diff);
  }

  auto mean() const -> double {
    double sum = 0;
    for (const auto& c : tdigests_[current_]) {
      sum += c.mean * c.weight;
    }
    return total_weight_ == 0 ? NAN : sum / total_weight_;
  }

  auto total_weight() const -> double {
    return total_weight_;
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
  : impl_(new tdigest_impl(delta)) {
  input_.reserve(buffer_size);
  reset();
}

tdigest::~tdigest() = default;
tdigest::tdigest(tdigest&&) = default;
tdigest& tdigest::operator=(tdigest&&) = default;

void tdigest::reset() {
  input_.resize(0);
  impl_->reset();
}

auto tdigest::validate() const -> std::expected<void, std::string> {
  merge_input();
  return impl_->validate();
}

void tdigest::dump() const {
  merge_input();
  impl_->dump();
}

void tdigest::merge(const std::vector<tdigest>& others) {
  merge_input();

  std::vector<const tdigest_impl*> other_impls;
  other_impls.reserve(others.size());
  for (auto& other : others) {
    other.merge_input();
    other_impls.push_back(other.impl_.get());
  }
  impl_->merge(other_impls);
}

void tdigest::merge(const tdigest& other) {
  merge_input();
  other.merge_input();
  impl_->merge({other.impl_.get()});
}

auto tdigest::quantile(double q) const -> double {
  merge_input();
  return impl_->quantile(q);
}

auto tdigest::mean() const -> double {
  merge_input();
  return impl_->mean();
}

auto tdigest::is_empty() const -> bool {
  return input_.size() == 0 && impl_->total_weight() == 0;
}

void tdigest::merge_input() const {
  if (input_.size() > 0) {
    impl_->merge_input(input_); // will mutate input_
  }
}

} // namespace tenzir::detail
