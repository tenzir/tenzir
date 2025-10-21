//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/bloom_filter_synopsis.hpp"

#include "tenzir/hash/xxhash.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/test/synopsis.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/builder.h>

using namespace tenzir;
using namespace si_literals;
using namespace test;

namespace {

// Helper to create a series from multiple int64_t values
auto make_int64_series(std::vector<int64_t> values) -> series {
  auto builder = arrow::Int64Builder{};
  for (auto value : values) {
    auto status = builder.Append(value);
    TENZIR_ASSERT(status.ok());
  }
  auto result = builder.Finish();
  TENZIR_ASSERT(result.ok());
  return series{type{int64_type{}}, std::move(*result)};
}

} // namespace

TEST("bloom filter parameters : from type") {
  auto t = type{ip_type{}, {{"synopsis", "bloomfilter(1000,0.01)"}}};
  auto xs = unbox(parse_parameters(t));
  CHECK_EQUAL(*xs.n, 1000u);
  CHECK_EQUAL(*xs.p, 0.01);
}

TEST("bloom filter synopsis") {
  using namespace nft;
  bloom_filter_parameters xs;
  xs.m = 1_k;
  xs.p = 0.1;
  auto bf = unbox(make_bloom_filter<xxh64>(std::move(xs)));
  bloom_filter_synopsis<int64_t, xxh64> x{type{int64_type{}}, std::move(bf)};
  x.add(make_int64_series({0, 1, 2}));
  auto verify = verifier{&x};
  MESSAGE("{{0, 1, 2}}");
  verify(make_data_view(int64_t{0}), {N, N, N, N, T, N, N, N, N, N});
  verify(make_data_view(int64_t{1}), {N, N, N, N, T, N, N, N, N, N});
  verify(make_data_view(int64_t{2}), {N, N, N, N, T, N, N, N, N, N});
  verify(make_data_view(int64_t{42}), {N, N, N, N, F, N, N, N, N, N});
}

TEST("bloom filter synopsis - wrong lookup type") {
  bloom_filter_parameters xs;
  xs.m = 1_k;
  xs.p = 0.1;
  auto bf = unbox(make_bloom_filter<xxh64>(std::move(xs)));
  bloom_filter_synopsis<std::string, xxh64> synopsis{type{string_type{}},
                                                     std::move(bf)};
  auto r1
    = synopsis.lookup(relational_operator::equal, make_data_view(caf::none));
  CHECK_EQUAL(r1, std::nullopt);
  auto r2
    = synopsis.lookup(relational_operator::equal, make_data_view(int64_t{17}));
  CHECK_EQUAL(r2, false);
}
