//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE blocked_bloom_filter

#include "vast/sketches/blocked_bloom_filter.hpp"

#include "vast/concept/hashable/hash.hpp"
#include "vast/concepts.hpp"
#include "vast/si_literals.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace si_literals;

TEST(construct with size in bytes) {
  blocked_bloom_filter filter{1_Ki};
  CHECK_EQUAL(as_bytes(filter).size(), 1_Ki);
  filter.add(hash(42));
  filter.add(hash(42));
  filter.add(hash(43));
  filter.add(hash(44));
  CHECK(filter.lookup(hash(42)));
  CHECK(filter.lookup(hash(43)));
  CHECK(filter.lookup(hash(44)));
  CHECK(!filter.lookup(hash(1337)));
}

TEST(construct with n and p) {
  blocked_bloom_filter filter{100, 0.01};
  CHECK_EQUAL(as_bytes(filter).size(), 160u);
  filter.add(hash(42));
  filter.add(hash(42));
  filter.add(hash(43));
  filter.add(hash(44));
  CHECK(filter.lookup(hash(42)));
  CHECK(filter.lookup(hash(43)));
  CHECK(filter.lookup(hash(44)));
  CHECK(!filter.lookup(hash(1337)));
}
