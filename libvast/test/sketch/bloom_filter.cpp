//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE bloom_filter

#include "vast/sketch/bloom_filter.hpp"

#include "vast/bloom_filter_parameters.hpp"
#include "vast/hash/hash.hpp"
#include "vast/si_literals.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

#include <cmath>
#include <random>
#include <vector>

using namespace vast;
using namespace si_literals;
using namespace decimal_byte_literals;

TEST(bloom filter api and memory usage) {
  bloom_filter_parameters xs;
  xs.m = 1_kB;
  xs.p = 0.1;
  auto filter = unbox(sketch::bloom_filter::make(xs));
  filter.add(hash("foo"));
  CHECK(filter.lookup(hash("foo")));
  CHECK(!filter.lookup(hash("bar")));
  auto m = *filter.parameters().m;
  auto mem = sizeof(bloom_filter_parameters) + sizeof(std::vector<uint64_t>)
             + ((m + 63) / 64 * sizeof(uint64_t));
  CHECK_EQUAL(mem_usage(filter), mem);
}

TEST(bloom filter fp test) {
  bloom_filter_parameters xs;
  xs.n = 10_k;
  xs.p = 0.1;
  auto filter = unbox(sketch::bloom_filter::make(xs));
  auto params = filter.parameters();
  std::mt19937_64 r{0};
  auto num_fps = 0u;
  auto num_queries = 1_M;
  // Load filter to full capacity.
  for (size_t i = 0; i < *params.n; ++i)
    filter.add(hash(r()));
  // Sample.
  for (size_t i = 0; i < num_queries; ++i)
    if (filter.lookup(hash(r())))
      ++num_fps;
  auto p = *params.p;
  auto p_hat = static_cast<double>(num_fps) / num_queries;
  auto epsilon = 0.001;
  CHECK_LESS(std::abs(p_hat - p), epsilon);
}
