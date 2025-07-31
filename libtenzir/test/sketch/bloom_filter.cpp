//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/sketch/bloom_filter.hpp"

#include "tenzir/hash/hash.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/test/test.hpp"

#include <fmt/format.h>

#include <cmath>
#include <random>
#include <vector>

using namespace tenzir;
using namespace tenzir::sketch;
using namespace si_literals;
using namespace decimal_byte_literals;

TEST("bloom filter api") {
  bloom_filter_config cfg;
  cfg.n = 1_k;
  cfg.p = 0.1;
  auto filter = unbox(bloom_filter::make(cfg));
  filter.add(hash("foo"));
  CHECK(filter.lookup(hash("foo")));
  CHECK(! filter.lookup(hash("bar")));
}

TEST("bloom filter odd m") {
  bloom_filter_config cfg;
  cfg.m = 1'024;
  cfg.p = 0.1;
  auto filter = unbox(bloom_filter::make(cfg));
  CHECK(filter.parameters().m & 1);
}

TEST("bloom filter fp test") {
  bloom_filter_config cfg;
  cfg.n = 10_k;
  cfg.p = 0.1;
  auto filter = unbox(bloom_filter::make(cfg));
  auto params = filter.parameters();
  std::mt19937_64 r{0};
  auto num_fps = 0u;
  auto num_queries = 1_M;
  // Load filter to full capacity.
  for (size_t i = 0; i < params.n; ++i) {
    filter.add(hash(r()));
  }
  // Sample true negatives.
  for (size_t i = 0; i < num_queries; ++i) {
    if (filter.lookup(hash(r()))) {
      ++num_fps;
    }
  }
  auto p = params.p;
  auto p_hat = static_cast<double>(num_fps) / num_queries;
  auto epsilon = 0.001;
  CHECK_LESS(std::abs(p_hat - p), epsilon);
}

TEST("frozen bloom filter") {
  bloom_filter_config cfg;
  cfg.m = 1_kB;
  cfg.p = 0.1;
  auto filter = unbox(bloom_filter::make(cfg));
  filter.add(hash("foo"));
  CHECK(filter.lookup(hash("foo")));
  auto frozen = unbox(freeze(filter));
  CHECK(frozen.lookup(hash("foo")));
  CHECK_EQUAL(filter.parameters(), frozen.parameters());
}
