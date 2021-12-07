//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/sketch/buffered_builder.hpp"
#include "vast/sketch/sketch.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstdint>
#include <unordered_set>

namespace vast::sketch {

class sketch;

/// Builds an optimally sized Bloom filter for a given FP probability.
class bloom_filter_builder final : public buffered_builder {
public:
  /// Constructs a Bloom filter builder.
  /// @param p The false-positive probability for optimal sizing on finish.
  explicit bloom_filter_builder(double p);

  caf::expected<sketch>
  build(const std::unordered_set<uint64_t>& digests) final;

private:
  double p_;
};

} // namespace vast::sketch
