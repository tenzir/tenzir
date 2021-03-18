// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE bloom_filter_synopsis

#include "vast/bloom_filter_synopsis.hpp"

#include "vast/test/synopsis.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/si_literals.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace si_literals;
using namespace test;

TEST(bloom filter parameters : from type) {
  auto t = address_type{}.attributes({{"synopsis", "bloomfilter(1000,0.01)"}});
  auto xs = unbox(parse_parameters(t));
  CHECK_EQUAL(*xs.n, 1000u);
  CHECK_EQUAL(*xs.p, 0.01);
}

TEST(bloom filter synopsis) {
  using namespace nft;
  bloom_filter_parameters xs;
  xs.m = 1_k;
  xs.p = 0.1;
  auto bf = unbox(make_bloom_filter<xxhash64>(std::move(xs)));
  bloom_filter_synopsis<integer, xxhash64> x{integer_type{}, std::move(bf)};
  x.add(make_data_view(0));
  x.add(make_data_view(1));
  x.add(make_data_view(2));
  auto verify = verifier{&x};
  MESSAGE("{0, 1, 2}");
  verify(make_data_view(0), {N, N, N, N, N, N, T, N, N, N, N, N});
  verify(make_data_view(1), {N, N, N, N, N, N, T, N, N, N, N, N});
  verify(make_data_view(2), {N, N, N, N, N, N, T, N, N, N, N, N});
  verify(make_data_view(42), {N, N, N, N, N, N, F, N, N, N, N, N});
}
