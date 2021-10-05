//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE blocked_bloom_filter

#include "vast/sketches/blocked_bloom_filter.hpp"

#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/si_literals.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace vast::sketches;
using namespace si_literals;
using namespace binary_byte_literals;
using namespace decimal_byte_literals;

TEST(basic add and lookup) {
  blocked_bloom_filter<xxhash64> filter{1024};
  filter.add(42);
  filter.add(43);
  filter.add(44);
  CHECK(filter.lookup(42));
  CHECK(filter.lookup(43));
  CHECK(filter.lookup(44));
  CHECK(!filter.lookup(1337));
}
