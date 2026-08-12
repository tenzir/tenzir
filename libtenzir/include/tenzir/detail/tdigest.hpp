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

// approximate quantiles from arbitrary length dataset with O(1) space
// based on 'Computing Extremely Accurate Quantiles Using t-Digests' from
// Dunning & Ertl
// - https://arxiv.org/abs/1902.04023
// - https://github.com/tdunning/t-digest

#pragma once

#include "tenzir/detail/assert.hpp"

#include <cmath>
#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace tenzir::detail {

// serialized centroid state of a tdigest, used for snapshotting
struct tdigest_state {
  std::vector<double> means;
  std::vector<double> weights;
  double min = 0;
  double max = 0;
};

class tdigest {
public:
  explicit tdigest(uint32_t delta = 100, uint32_t buffer_size = 500);
  ~tdigest();
  tdigest(tdigest&&);
  tdigest& operator=(tdigest&&);

  // reset and re-use this tdigest
  auto reset() -> void;

  // validate data integrity
  auto validate() const -> std::expected<void, std::string>;

  // dump internal data, only for debug
  auto dump() const -> void;

  // Buffer a single finite data point, consuming the internal buffer if full.
  // This function is intensively called and performance critical. Call it only
  // if the input data is known to be finite.
  auto add(double value) -> void {
    TENZIR_ASSERT(std::isfinite(value), "cannot add a non-finite value");
    if (input_.size() == input_.capacity()) [[unlikely]] {
      merge_input();
    }
    input_.push_back(value);
  }

  // Skip non-finite floating-point values when adding.
  template <class T>
  auto finite_add(T value) -> std::enable_if_t<std::is_floating_point_v<T>> {
    if (std::isfinite(value)) {
      add(value);
    }
  }

  template <class T>
  auto finite_add(T value) -> std::enable_if_t<std::is_integral_v<T>> {
    add(static_cast<double>(value));
  }

  // merge with other t-digests, called infrequently
  auto merge(const std::vector<tdigest>& others) -> void;
  auto merge(const tdigest& other) -> void;

  // calculate quantile
  auto quantile(double q) const -> double;

  auto min() const -> double {
    return quantile(0);
  }
  auto max() const -> double {
    return quantile(1);
  }
  auto mean() const -> double;

  // check if this tdigest contains no valid data points
  auto is_empty() const -> bool;

  // serialize the merged centroid state for snapshotting; flushes buffered
  // input first
  auto save() const -> tdigest_state;

  // rebuild the digest from a serialized state; returns false and resets the
  // digest if the state is invalid
  auto restore(const tdigest_state& state) -> bool;

private:
  // merge input data with current tdigest
  auto merge_input() const -> void;

  // input buffer, size = buffer_size * sizeof(double)
  mutable std::vector<double> input_;

  // hide other members with pimpl
  class tdigest_impl;
  std::unique_ptr<tdigest_impl> impl_;
};

} // namespace tenzir::detail
