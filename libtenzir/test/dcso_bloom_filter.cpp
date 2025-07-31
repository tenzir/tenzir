//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/dcso_bloom_filter.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/dcso_bloom_hasher.hpp"
#include "tenzir/hash/uhash.hpp"
#include "tenzir/test/test.hpp"

#include <cstdlib>
#include <string>
#include <string_view>

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace tenzir;

namespace {

TEST("dcso bloom parameterization") {
  // Lower bound on number of cells. Indeed, 1-bit filter is technically
  // possible.
  CHECK_EQUAL(dcso_bloom_filter::m(1, 0.9), 0u);
  CHECK_EQUAL(dcso_bloom_filter::m(2, 0.9), 0u);
  CHECK_EQUAL(dcso_bloom_filter::m(4, 0.9), 0u);
  CHECK_EQUAL(dcso_bloom_filter::m(1, 0.5), 1u);
  CHECK_EQUAL(dcso_bloom_filter::m(1, 0.1), 4u);
  CHECK_EQUAL(dcso_bloom_filter::m(8, 0.1), 38u);
  CHECK_EQUAL(dcso_bloom_filter::m(1, 0.5), 1u);
  CHECK_EQUAL(dcso_bloom_filter::k(1, 0.5), 1u);
}

TEST("dcso bloom default construction") {
  dcso_bloom_filter filter;
  CHECK_EQUAL(filter.parameters().m, 0u);
  CHECK_EQUAL(filter.parameters().n, 0u);
  CHECK_EQUAL(filter.parameters().k, 1u);
  CHECK_EQUAL(filter.parameters().p, 1.0);
}

// https://github.com/DCSO/bloom/blob/9240e18c9363ee935edbdf025c07e4f3cca43b1d/bloom_test.go#L18
TEST("dcso bloom fingerprinting") {
  dcso_bloom_hasher<fnv1<64>> hasher{7};
  auto digests = hasher("bar"s);
  auto expected
    = decltype(digests){20311, 36825, 412501, 835777, 658914, 853361, 307361};
  // DCSO's Bloom filter performs the mod-m operation as part of the digest
  // (fingerprint) computation. Tenzir does it within the Bloom filter
  // implementation because it may vary based on the partitioning policy.
  for (auto& digest : digests) {
    digest %= dcso_bloom_filter::m(100'000, 0.01);
  }
  CHECK_EQUAL(digests, expected);
}

// https://github.com/DCSO/bloom/blob/9240e18c9363ee935edbdf025c07e4f3cca43b1d/bloom_test.go#L31
TEST("dcso bloom initialization") {
  dcso_bloom_filter filter{10'000, 0.001};
  auto params = filter.parameters();
  CHECK_EQUAL(*params.n, 10'000u);
  CHECK_EQUAL(*params.p, 0.001);
  CHECK_EQUAL(*params.k, 10u);
  CHECK_EQUAL(*params.m, 143775u);
}

// https://github.com/DCSO/bloom/blob/9240e18c9363ee935edbdf025c07e4f3cca43b1d/bloom_test.go#L209
auto generate_test_value(size_t length) -> std::vector<std::byte> {
  auto make_random_byte = [] {
    // auto byte = std::experimental::randint<int>(0, 255);
    auto byte = std::rand() % 256;
    return static_cast<std::byte>(byte);
  };
  std::vector<std::byte> result(length);
  for (auto& byte : result) {
    byte = make_random_byte();
  }
  return result;
}

// https://github.com/DCSO/bloom/blob/9240e18c9363ee935edbdf025c07e4f3cca43b1d/bloom_test.go#L217
auto generate_example_filter(uint64_t capacity, double p, size_t num_samples)
  -> std::pair<dcso_bloom_filter, std::vector<std::vector<std::byte>>> {
  auto filter = dcso_bloom_filter{capacity, p};
  // Attach "foobar" data to the filter.
  std::vector<std::byte> foobar = {
    std::byte{'f'}, std::byte{'o'}, std::byte{'o'},
    std::byte{'b'}, std::byte{'a'}, std::byte{'r'},
  };
  filter.data() = foobar;
  // Generate test values.
  std::vector<std::vector<std::byte>> test_values(num_samples);
  for (auto& test_value : test_values) {
    test_value = generate_test_value(100);
    filter.add(test_value);
  }
  return {filter, test_values};
}

// https://github.com/DCSO/bloom/blob/9240e18c9363ee935edbdf025c07e4f3cca43b1d/bloom_test.go#L244
TEST("dcso bloom checking") {
  auto [filter, values] = generate_example_filter(100'000, 0.001, 100'000);
  for (const auto& value : values) {
    if (! filter.lookup(value)) {
      FAIL("expected value not present in filter:{}", value);
    }
  }
}

// https://github.com/DCSO/bloom/blob/9240e18c9363ee935edbdf025c07e4f3cca43b1d/bloom_test.go#L91
TEST("dcso bloom serialization") {
  auto [x, _] = generate_example_filter(100'000, 0.01, 1'000);
  x.data() = {std::byte{'\x2a'}, std::byte{'\x2a'}, std::byte{'\x2a'}};
  dcso_bloom_filter y;
  std::vector<std::byte> buffer;
  CHECK_EQUAL(convert(x, buffer), caf::none);
  REQUIRE_EQUAL(convert(as_bytes(buffer), y), caf::none);
  CHECK_EQUAL(x, y);
  // Add one more value, rinse, repeat.
  auto value = generate_test_value(100);
  x.add(value);
  y.add(value);
  buffer.clear();
  CHECK_EQUAL(convert(x, buffer), caf::none);
  CHECK_EQUAL(convert(as_bytes(buffer), y), caf::none);
  CHECK_EQUAL(x, y);
}

TEST("dcso bloom binary equivalence") {
  // Generated the baseline as follows:
  // - bloom create -p 0.1 -n 100 ns.bloom
  // - echo "1.1.1.1,8.8.8.8" | bloom -s insert ns.bloom
  // - echo foo | bloom set-data ns.bloom
  // - xxd -i ns.bloom
  unsigned char ns_bloom[]
    = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xb9, 0x3f,
       0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xdf, 0x01, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40, 0x90, 0x02, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x66, 0x6f, 0x6f, 0x0a};
  unsigned int ns_bloom_len = 116;
  auto bloom_buffer = std::span<const std::byte>{
    reinterpret_cast<const std::byte*>(ns_bloom), ns_bloom_len};
  // Generate the same with our implementation.
  dcso_bloom_filter x{100, 0.1};
  x.add(view<data>{"1.1.1.1"sv});
  x.add(view<data>{"8.8.8.8"sv});
  // 'echo' included a newline.
  auto foo = std::vector<std::byte>{std::byte('f'), std::byte('o'),
                                    std::byte('o'), std::byte('\n')};
  x.data() = foo;
  std::vector<std::byte> buffer;
  CHECK_EQUAL(convert(x, buffer), caf::none);
  CHECK_EQUAL(as_bytes(buffer), bloom_buffer);
}

} // namespace
