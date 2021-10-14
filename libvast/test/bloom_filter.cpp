//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE bloom_filter

#include "vast/bloom_filter.hpp"

#include "vast/bloom_filter_parameters.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/detail/deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/si_literals.hpp"
#include "vast/test/test.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/test/dsl.hpp>

#include <cmath>
#include <string>

using namespace vast;
using namespace si_literals;
using namespace binary_byte_literals;
using namespace decimal_byte_literals;

namespace vast::test::detail {

template <int precision_bits>
struct almost_equal {
  template <class T1, class T2>
  bool operator()(const T1& x, const T2& y) {
    auto e = std::ilogb(y);
    auto th = std::fmod(y, std::ldexp(1, e - precision_bits));
    return std::fabs(x - y) <= th;
  }
};

} // namespace vast::test::detail

#define CHECK_ALMOST_EQUAL(x, y, p)                                            \
  CAF_CHECK_FUNC(vast::test::detail::almost_equal<(p)>, (x), (y))

// Ground truth for the parameters stem from https://hur.st/bloomfilter.

namespace {
constexpr const int precision_bits = 20;
}

TEST(bloom filter parameters : mnk) {
  bloom_filter_parameters xs;
  xs.m = 42_k;
  xs.n = 5_k;
  xs.k = 7;
  auto ys = evaluate(xs);
  REQUIRE(ys);
  REQUIRE(ys->m);
  REQUIRE(ys->n);
  REQUIRE(ys->k);
  REQUIRE(ys->p);
  CHECK_EQUAL(*ys->m, 42_k);
  CHECK_EQUAL(*ys->n, 5_k);
  CHECK_EQUAL(*ys->k, 7u);
  CHECK_ALMOST_EQUAL(*ys->p, 0.018471419, precision_bits);
}

TEST(bloom filter parameters : np) {
  bloom_filter_parameters xs;
  xs.n = 1_M;
  xs.p = 0.01;
  auto ys = evaluate(xs);
  REQUIRE(ys);
  REQUIRE(ys->m);
  REQUIRE(ys->n);
  REQUIRE(ys->k);
  REQUIRE(ys->p);
  CHECK_EQUAL(*ys->m, 9'585'059u);
  CHECK_EQUAL(*ys->n, 1_M);
  CHECK_EQUAL(*ys->k, 7u);
  CHECK_ALMOST_EQUAL(*ys->p, 0.010039215, precision_bits);
}

TEST(bloom filter parameters : mn) {
  bloom_filter_parameters xs;
  xs.m = 20_M;
  xs.n = 7_M;
  auto ys = evaluate(xs);
  REQUIRE(ys);
  REQUIRE(ys->m);
  REQUIRE(ys->n);
  REQUIRE(ys->k);
  REQUIRE(ys->p);
  CHECK_EQUAL(*ys->m, 20_M);
  CHECK_EQUAL(*ys->n, 7_M);
  CHECK_EQUAL(*ys->k, 2u);
  CHECK_ALMOST_EQUAL(*ys->p, 0.253426356, precision_bits);
}

TEST(bloom filter parameters : mp) {
  bloom_filter_parameters xs;
  xs.m = 10_M;
  xs.p = 0.001;
  auto ys = evaluate(xs);
  REQUIRE(ys);
  REQUIRE(ys->m);
  REQUIRE(ys->n);
  REQUIRE(ys->k);
  REQUIRE(ys->p);
  CHECK_EQUAL(*ys->m, 10_M);
  CHECK_EQUAL(*ys->n, 695'527u);
  CHECK_EQUAL(*ys->k, 10u);
  CHECK_ALMOST_EQUAL(*ys->p, 0.001000025, precision_bits);
}

TEST(bloom filter parameters : from string) {
  auto xs = vast::test::unbox(parse_parameters("bloomfilter(1000,0.01)"));
  CHECK_EQUAL(*xs.n, 1000u);
  CHECK_EQUAL(*xs.p, 0.01);
  CHECK(!xs.m);
  CHECK(!xs.k);
  auto ys = evaluate(xs);
  CHECK_EQUAL(*ys->m, 9586u);
  CHECK_EQUAL(*ys->n, 1000u);
  CHECK_EQUAL(*ys->k, 7u);
  CHECK_ALMOST_EQUAL(*ys->p, 0.010034532, precision_bits);
}

TEST(simple_hasher) {
  auto h = simple_hasher<xxh64>{2, {0, 1}};
  auto& xs = h(42);
  REQUIRE_EQUAL(h.size(), 2u);
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs[0], 15516826743637085169ul);
  CHECK_EQUAL(xs[1], 1813822717707961023ul);
  MESSAGE("persistence");
  std::vector<char> buf;
  auto err = detail::serialize(buf, h);
  REQUIRE_EQUAL(err, caf::none);
  simple_hasher<xxh64> g;
  err = detail::deserialize(buf, g);
  REQUIRE_EQUAL(err, caf::none);
  CHECK(h == g);
}

TEST(double_hasher) {
  auto h = double_hasher<xxh64>{4, {1337, 4711}};
  auto& xs = h(42);
  REQUIRE_EQUAL(h.size(), 4u);
  REQUIRE_EQUAL(xs.size(), 4u);
  CHECK_EQUAL(xs[0], 340423191260729621ul);
  CHECK_EQUAL(xs[1], 13661102917286555827ul);
  CHECK_EQUAL(xs[2], 8535038569602830417ul);
  CHECK_EQUAL(xs[3], 3408974221919105007ul);
  MESSAGE("persistence");
  std::vector<char> buf;
  auto err = detail::serialize(buf, h);
  REQUIRE_EQUAL(err, caf::none);
  double_hasher<xxh64> g;
  err = detail::deserialize(buf, g);
  REQUIRE_EQUAL(err, caf::none);
  CHECK(h == g);
}

TEST(bloom filter - default - constructed) {
  bloom_filter<xxh64> x;
  CHECK(x.size() == 0u);
}

TEST(bloom filter - constructed from parameters) {
  bloom_filter_parameters xs;
  xs.m = 10_M;
  xs.p = 0.001;
  auto x = vast::test::unbox(make_bloom_filter<xxh64>(xs));
  CHECK_EQUAL(x.size(), 10_M);
  x.add(42);
  x.add("foo");
  x.add(3.14);
  CHECK(x.lookup(42));
  CHECK(x.lookup("foo"));
  CHECK(x.lookup(3.14));
}

TEST(bloom filter - simple hasher and partitioning) {
  bloom_filter_parameters xs;
  xs.m = 10_M;
  xs.p = 0.001;
  auto x = vast::test::unbox(
    make_bloom_filter<xxh64, simple_hasher, policy::partitioning::yes>(xs));
  CHECK_EQUAL(x.size(), 10_M);
  CHECK_EQUAL(x.num_hash_functions(), 10u);
  x.add(42);
  x.add("foo");
  x.add(3.14);
  CHECK(x.lookup(42));
  CHECK(x.lookup("foo"));
  CHECK(x.lookup(3.14));
  MESSAGE("persistence");
  std::vector<char> buf;
  auto err = detail::serialize(buf, x);
  REQUIRE_EQUAL(err, caf::none);
  bloom_filter<xxh64, simple_hasher, policy::partitioning::yes> y;
  err = detail::deserialize(buf, y);
  REQUIRE_EQUAL(err, caf::none);
  CHECK(x == y);
}

TEST(bloom filter - duplicate tracking) {
  bloom_filter_parameters xs;
  xs.m = 1_M;
  xs.p = 0.1;
  auto x = vast::test::unbox(
    make_bloom_filter<xxh64, double_hasher, policy::partitioning::no>(xs));
  CHECK(!x.lookup(42));
  CHECK(x.add(42));
  CHECK(x.lookup(42));
  CHECK(!x.add(42));
}
